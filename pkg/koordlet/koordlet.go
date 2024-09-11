/*
Copyright 2022 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package agent

import (
	"fmt"
	"os"
	"time"

	topologyclientset "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	clientsetbeta1 "github.com/koordinator-sh/koordinator/pkg/client/clientset/versioned"
	"github.com/koordinator-sh/koordinator/pkg/client/clientset/versioned/typed/scheduling/v1alpha1"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/config"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metrics"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metricsadvisor"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/prediction"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/qosmanager"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/resourceexecutor"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/runtimehooks"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer"
	statesinformerimpl "github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer/impl"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/util/system"
	"github.com/koordinator-sh/koordinator/pkg/util"
)

var (
	scheme = apiruntime.NewScheme()
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)
}

type Daemon interface {
	Run(stopCh <-chan struct{})
}

type daemon struct {
	metricAdvisor  metricsadvisor.MetricAdvisor
	statesInformer statesinformer.StatesInformer
	metricCache    metriccache.MetricCache
	qosManager     qosmanager.QOSManager
	runtimeHook    runtimehooks.RuntimeHook
	predictServer  prediction.PredictServer
	executor       resourceexecutor.ResourceUpdateExecutor
}

func NewDaemon(config *config.Configuration) (Daemon, error) {
	// get node name
	nodeName := os.Getenv("NODE_NAME")
	if len(nodeName) == 0 {
		return nil, fmt.Errorf("failed to new daemon: NODE_NAME env is empty")
	}
	klog.Infof("NODE_NAME is %v, start time %v", nodeName, float64(time.Now().Unix()))
	metrics.RecordKoordletStartTime(nodeName, float64(time.Now().Unix()))

	// TODO 应该为了方便排查问题增加的一些观测点
	system.InitSupportConfigs()
	klog.Infof("sysconf: %+v, agentMode: %v", system.Conf, system.AgentMode)
	klog.Infof("kernel version INFO: %+v", system.HostSystemInfo)

	// 初始化各种客户端工具，后续需要和APIServer通信获取关心的资源
	kubeClient := clientset.NewForConfigOrDie(config.KubeRestConf)
	crdClient := clientsetbeta1.NewForConfigOrDie(config.KubeRestConf)
	topologyClient := topologyclientset.NewForConfigOrDie(config.KubeRestConf)
	schedulingClient := v1alpha1.NewForConfigOrDie(config.KubeRestConf)

	// 1、MetricCache其实就是Koordlet架构图中的Storage，用于存储StateInfo以及Metrics
	// 2、就像介绍一样，MetricCache主要用于存储两种类型的数据，一种是时间数据，通过TSDBStorage存储，主要是指标，
	// 时间序列类型存储历史数据用于统计目的，例如 CPU 和内存使用情况；另外一种是KV类型的数据，也被称之为静态类型数据，
	// 静态类型包括节点、Pod 和容器的状态信息，例如节点的 CPU 信息、Pod 的元数据。
	metricCache, err := metriccache.NewMetricCache(config.MetricCacheConf)
	if err != nil {
		return nil, err
	}
	// 1、PredictServer本质上其实就是为了预测Pod, Node在某一时刻的使用量，这个预测的方式其实也并不难，其实就是统计学中的直方图采样，
	// 通过不断的给模型喂当前Pod以及node的内存，CPU使用情况，从而预测将来Pod以及Node在某时刻的使用情况。
	// 2、已经训练的模型是非常珍贵的，因为里面的历史数据已经为模型的准确性做了一些支撑，所谓在模型不断训练过程中，需要定期持久化模型，这样
	// 万一Koordlet在重启之后，很有可能造成模型丢失，从而需要重投开始训练模型。而PredictServer通过checkpointer定期持久化模型，
	// 这样方便Koordlet在重启之后能够恢复尽可能准确的模型
	predictServer := prediction.NewPeakPredictServer(config.PredictionConf)
	predictorFactory := prediction.NewPredictorFactory(predictServer, config.PredictionConf.ColdStartDuration,
		config.PredictionConf.SafetyMarginPercent)

	// TODO 缓存了什么内容？
	statesInformer := statesinformerimpl.NewStatesInformer(config.StatesInformerConf, kubeClient, crdClient,
		topologyClient, metricCache, nodeName, schedulingClient, predictorFactory)

	// TODO cgroupfs驱动，systemd驱动，这两种类型的CGroup驱动有何不同？ 大多数时候直接使用systemd cgroup驱动即可
	cgroupDriver := system.GetCgroupDriver()
	system.SetupCgroupPathFormatter(cgroupDriver)

	// 指标采集，想要做好在离混布，潮汐调度等功能，必须要把服务的资源画像做好，后续才能根据服务的资源画像提升机器的资源利用率
	collectorService := metricsadvisor.NewMetricAdvisor(config.CollectorConf, statesInformer, metricCache)

	// TODO 这玩意是啥？ 这里似乎在获取Pod API驱逐版本，估计是不同的版本之间存在差异
	evictVersion, err := util.FindSupportedEvictVersion(kubeClient)
	if err != nil {
		return nil, err
	}

	// TODO QOSManager原理分析
	qosManager := qosmanager.NewQOSManager(config.QOSManagerConf, scheme, kubeClient, crdClient, nodeName, statesInformer,
		metricCache, config.CollectorConf, evictVersion)

	// TODO 这里应该才是KoordRuntimeProxy
	runtimeHook, err := runtimehooks.NewRuntimeHook(statesInformer, config.RuntimeHookConf)
	if err != nil {
		return nil, err
	}

	d := &daemon{
		metricAdvisor:  collectorService,
		statesInformer: statesInformer,
		metricCache:    metricCache,
		qosManager:     qosManager,
		runtimeHook:    runtimeHook,
		predictServer:  predictServer,
		executor:       resourceexecutor.NewResourceUpdateExecutor(),
	}

	return d, nil
}

func (d *daemon) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	klog.Infof("Starting daemon")

	// start resource executor cache
	// TODO executor是干嘛的？
	d.executor.Run(stopCh)

	go func() {
		if err := d.metricCache.Run(stopCh); err != nil {
			klog.Fatal("Unable to run the metric cache: ", err)
		}
	}()

	// start states informer
	go func() {
		if err := d.statesInformer.Run(stopCh); err != nil {
			klog.Fatal("Unable to run the states informer: ", err)
		}
	}()
	// wait for metric advisor sync
	if !cache.WaitForCacheSync(stopCh, d.statesInformer.HasSynced) {
		klog.Fatal("time out waiting for states informer to sync")
	}

	// start metric advisor
	go func() {
		if err := d.metricAdvisor.Run(stopCh); err != nil {
			klog.Fatal("Unable to run the metric advisor: ", err)
		}
	}()
	// wait for metric advisor sync
	if !cache.WaitForCacheSync(stopCh, d.metricAdvisor.HasSynced) {
		klog.Fatal("time out waiting for metric advisor to sync")
	}

	// start predict server
	go func() {
		if err := d.predictServer.Setup(d.statesInformer, d.metricCache); err != nil {
			klog.Fatal("Unable to setup the predict server: ", err)
		}
		if err := d.predictServer.Run(stopCh); err != nil {
			klog.Fatal("Unable to run the predict server: ", err)
		}
	}()

	// start qos manager
	go func() {
		if err := d.qosManager.Run(stopCh); err != nil {
			klog.Fatal("Unable to run the qosManager: ", err)
		}
	}()

	go func() {
		if err := d.runtimeHook.Run(stopCh); err != nil {
			klog.Fatal("Unable to run the runtimeHook: ", err)
		}
	}()

	klog.Info("Start daemon successfully")
	<-stopCh
	klog.Info("Shutting down daemon")
}
