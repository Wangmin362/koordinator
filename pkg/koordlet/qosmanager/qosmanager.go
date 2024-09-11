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

package qosmanager

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	koordclientset "github.com/koordinator-sh/koordinator/pkg/client/clientset/versioned"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	_ "github.com/koordinator-sh/koordinator/pkg/koordlet/metrics"
	ma "github.com/koordinator-sh/koordinator/pkg/koordlet/metricsadvisor/framework"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/qosmanager/framework"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/qosmanager/plugins"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/resourceexecutor"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer"
)

type QOSManager interface {
	Run(stopCh <-chan struct{}) error
}

type qosManager struct {
	// QOSManager所依赖的组件，TODO 我觉得这里称之为Options并不合适，参数是参数，依赖的组件应该是依赖的组件
	options *framework.Options
	context *framework.Context
}

func NewQOSManager(cfg *framework.Config, schema *apiruntime.Scheme, kubeClient clientset.Interface,
	crdClient *koordclientset.Clientset, nodeName string,
	statesInformer statesinformer.StatesInformer, metricCache metriccache.MetricCache,
	metricAdvisorConfig *ma.Config, evictVersion string) QOSManager {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&clientcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(schema, corev1.EventSource{Component: "koordlet-qosManager", Host: nodeName})
	cgroupReader := resourceexecutor.NewCgroupReader()

	// TODO 驱逐器，应该就是用来驱逐Pod组件， 1、什么时候需要驱逐一个Pod? 2、需要驱逐Pod的时候什么样的Pod会被驱逐？
	evictor := framework.NewEvictor(kubeClient, recorder, evictVersion)

	// 各个依赖的组件
	opt := &framework.Options{
		CgroupReader:        cgroupReader,
		StatesInformer:      statesInformer, // 用于和APIServer交互获取Pod, 容器，node相关数据并放入到MetricCache当中
		MetricCache:         metricCache,    // 用于缓存Pod,容器,node的时间数据以及静态数据
		EventRecorder:       recorder,
		KubeClient:          kubeClient,
		EvictVersion:        evictVersion,
		Config:              cfg,
		MetricAdvisorConfig: metricAdvisorConfig,
	}

	ctx := &framework.Context{
		Evictor:    evictor,
		Strategies: make(map[string]framework.QOSStrategy, len(plugins.StrategyPlugins)),
	}

	for name, strategyFn := range plugins.StrategyPlugins {
		// 实例化各个QOS策略插件
		ctx.Strategies[name] = strategyFn(opt)
	}

	r := &qosManager{
		options: opt,
		context: ctx,
	}
	return r
}

func (r *qosManager) setup() {
	for _, s := range r.context.Strategies {
		// 为各个QOS策略插件设置上下文，其实就是设置驱逐器以及其它插件？
		// TODO 这里为什么要这么设计？
		s.Setup(r.context)
	}
}

func (r *qosManager) Run(stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	// minimum interval is one second.
	// TODO 这里为什么要做这个检测，小于1秒钟会怎样？
	if r.options.MetricAdvisorConfig.CollectResUsedInterval < time.Second {
		klog.Infof("collectResUsedIntervalSeconds is %v, qos manager is disabled",
			r.options.MetricAdvisorConfig.CollectResUsedInterval)
		return nil
	}

	klog.Info("Starting qos manager")
	r.setup()

	// TODO 运行驱逐器
	err := r.context.Evictor.Start(stopCh)
	if err != nil {
		klog.Fatal("start evictor failed %v", err)
	}

	// TODO 这玩意目前看起来就是空的，没啥用。 什么叫做灰色控制？
	go framework.RunQOSGreyCtrlPlugins(r.options.KubeClient, stopCh)

	// StateInformer其实就是各个资源Informer的组合，这里其实就是在等到各个资源Informer同步完成
	if !cache.WaitForCacheSync(stopCh, r.options.StatesInformer.HasSynced) {
		return fmt.Errorf("time out waiting for states informer caches to sync")
	}

	for name, strategy := range r.context.Strategies {
		klog.V(4).Infof("ready to start qos strategy %v", name)
		if !strategy.Enabled() { // 插件没有启用的话，直接忽略，可以通过在启动的时候修改命令行参数启用需要的特性开关
			klog.V(4).Infof("qos strategy %v is not enabled, skip running", name)
			continue
		}

		// TODO 启动各个策略插件  QosManger的核心其实就是各个插件
		go strategy.Run(stopCh)
		klog.V(4).Infof("qos strategy %v start", name)
	}

	klog.Infof("start qos manager extensions")
	// 目前是空的，啥也没有
	framework.SetupPlugins(r.options.KubeClient, r.options.MetricCache, r.options.StatesInformer)
	utilruntime.Must(framework.StartPlugins(r.options.Config.QOSExtensionCfg, stopCh))

	klog.Info("Starting qosManager successfully")
	<-stopCh
	klog.Info("shutting down qosManager")
	return nil
}
