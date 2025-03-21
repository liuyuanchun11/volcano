/*
Copyright 2025 The Volcano Authors.

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

package pluginsinterface

import (
	kubeclientset "k8s.io/client-go/kubernetes"

	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

type PLuginClientset struct {
	KubeClients kubeclientset.Interface
}

type PluginInterface interface {
	Name() string

	OnRanktableCompleted(ranktable *cutil.RankTable, args map[string]interface{}) (*cutil.RankTable, error)

	OnRanktableDelete(args map[string]interface{}) error
}
