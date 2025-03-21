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

package ringcontroller

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"

	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func (rc *RankTblController) InitConfigmapInformer() error {
	labelSelector := labels.Set(map[string]string{cutil.Label1980Key: cutil.Label1980Value}).AsSelector().String()
	configmapFactory := informers.NewSharedInformerFactoryWithOptions(rc.clients.KubeClient, time.Second*30,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labelSelector
		}),
	)

	rc.configmapInformer = configmapFactory.Core().V1().ConfigMaps().Informer()
	rc.configmapLister = configmapFactory.Core().V1().ConfigMaps().Lister()

	return nil
}
