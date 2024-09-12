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

package options

const (
	DefaultRuntimeProxyEndpoint = "/var/run/koord-runtimeproxy/runtimeproxy.sock"

	DefaultContainerdRuntimeServiceEndpoint = "/var/run/containerd/containerd.sock"

	BackendRuntimeModeContainerd = "Containerd"
	BackendRuntimeModeDocker     = "Docker"
	DefaultBackendRuntimeMode    = BackendRuntimeModeContainerd

	DefaultHookServerKey = "runtimeproxy.koordinator.sh/skip-hookserver"
	DefaultHookServerVal = "true"
)

var (
	// 这里是KoordRuntimeProxy真正监听的Sock地址，Kubelet调用CRI的时候就是调用的这里
	RuntimeProxyEndpoint string
	// 这里传入的是真正的CRI运行时，KoordRuntimeProxy拦截CRI请求，经过修改之后转发给真正的容器运行时，这里就是在指定真正的容器运行时
	RemoteRuntimeServiceEndpoint string

	// BackendRuntimeMode default to 'containerd'
	// containerd还是docker TODO 这里其实使用枚举来定义会更加好一些
	BackendRuntimeMode string

	RuntimeHookServerKey string
	RuntimeHookServerVal string
)
