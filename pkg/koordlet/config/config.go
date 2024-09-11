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

package config

import (
	"flag"
	"strings"

	"k8s.io/client-go/rest"
	cliflag "k8s.io/component-base/cli/flag"
	"sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/koordinator-sh/koordinator/pkg/features"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/audit"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	maframework "github.com/koordinator-sh/koordinator/pkg/koordlet/metricsadvisor/framework"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/prediction"
	qmframework "github.com/koordinator-sh/koordinator/pkg/koordlet/qosmanager/framework"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/resourceexecutor"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/runtimehooks"
	statesinformerimpl "github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer/impl"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/util/system"
)

const (
	DefaultKoordletConfigMapNamespace = "koordinator-system"
	DefaultKoordletConfigMapName      = "koordlet-config"

	CMKeyQoSPluginExtraConfigs = "qos-plugin-extra-configs"
)

type Configuration struct {
	ConfigMapName      string
	ConfigMapNamesapce string
	KubeRestConf       *rest.Config
	StatesInformerConf *statesinformerimpl.Config
	CollectorConf      *maframework.Config
	MetricCacheConf    *metriccache.Config
	QOSManagerConf     *qmframework.Config
	RuntimeHookConf    *runtimehooks.Config
	AuditConf          *audit.Config
	PredictionConf     *prediction.Config

	FeatureGates map[string]bool
}

func NewConfiguration() *Configuration {
	return &Configuration{
		ConfigMapName:      DefaultKoordletConfigMapName,
		ConfigMapNamesapce: DefaultKoordletConfigMapNamespace,
		StatesInformerConf: statesinformerimpl.NewDefaultConfig(),
		CollectorConf:      maframework.NewDefaultConfig(),
		MetricCacheConf:    metriccache.NewDefaultConfig(),
		QOSManagerConf:     qmframework.NewDefaultConfig(),
		RuntimeHookConf:    runtimehooks.NewDefaultConfig(),
		AuditConf:          audit.NewDefaultConfig(),
		PredictionConf:     prediction.NewDefaultConfig(),
	}
}

func (c *Configuration) InitFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ConfigMapName, "configmap-name", DefaultKoordletConfigMapName, "determines the name the koordlet configmap uses.")
	fs.StringVar(&c.ConfigMapNamesapce, "configmap-namespace", DefaultKoordletConfigMapNamespace, "determines the namespace of configmap uses.")
	system.Conf.InitFlags(fs)
	c.StatesInformerConf.InitFlags(fs)
	c.CollectorConf.InitFlags(fs)
	c.MetricCacheConf.InitFlags(fs)
	c.QOSManagerConf.InitFlags(fs)
	c.RuntimeHookConf.InitFlags(fs)
	c.AuditConf.InitFlags(fs)
	c.PredictionConf.InitFlags(fs)
	resourceexecutor.Conf.InitFlags(fs)
	fs.Var(cliflag.NewMapStringBool(&c.FeatureGates), "feature-gates", "A set of key=value pairs that describe feature gates for alpha/experimental features. "+
		"Options are:\n"+strings.Join(features.DefaultKoordletFeatureGate.KnownFeatures(), "\n"))
}

func (c *Configuration) InitKubeConfigForKoordlet(kubeAPIQPS float64, kubeAPIBurst int) error {
	// 加载kubeConfig配置我呢见
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	cfg.UserAgent = "koordlet"
	cfg.QPS = float32(kubeAPIQPS)
	cfg.Burst = kubeAPIBurst
	c.KubeRestConf = cfg
	return nil
}
