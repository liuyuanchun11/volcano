/*
Copyright 2024 The Volcano Authors.

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

package hyperjobpluginsinterface

import (
	"k8s.io/client-go/kubernetes"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	vcclient "volcano.sh/apis/pkg/client/clientset/versioned"
)

// PluginClientset clientset.
type PluginClientset struct {
	KubeClients kubernetes.Interface
	VcClients   vcclient.Interface
}

// PluginInterface interface.
type PluginInterface interface {
	// Name returns the unique name of Plugin.
	Name() string

	// OnHyperJobAdd is called when do hyperJob initiation
	// Note: it can be called multi times, must be idempotent
	OnHyperJobAdd(hyperJob *vcbatch.HyperJob) error

	// OnJobCreate is called when creating jobs
	OnJobCreate(job *vcbatch.Job, hyperJob *vcbatch.HyperJob) error
}
