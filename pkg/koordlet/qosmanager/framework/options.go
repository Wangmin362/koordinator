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

package framework

import (
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"

	"github.com/koordinator-sh/koordinator/pkg/koordlet/metriccache"
	ma "github.com/koordinator-sh/koordinator/pkg/koordlet/metricsadvisor/framework"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/resourceexecutor"
	"github.com/koordinator-sh/koordinator/pkg/koordlet/statesinformer"
)

// Options TODO 这里称之为选项，其实我觉得称之为依赖会更加合适，QOSManager的插件将会依赖这里的组件
type Options struct {
	CgroupReader        resourceexecutor.CgroupReader
	StatesInformer      statesinformer.StatesInformer
	MetricCache         metriccache.MetricCache
	EventRecorder       record.EventRecorder
	KubeClient          clientset.Interface
	EvictVersion        string
	Config              *Config
	MetricAdvisorConfig *ma.Config
}
