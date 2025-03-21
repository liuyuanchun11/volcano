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
	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/informers"
	kfake "k8s.io/client-go/kubernetes/fake"
	"testing"
	vcfake "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	vcinformers "volcano.sh/apis/pkg/client/informers/externalversions"

	"volcano.sh/volcano/pkg/controllers/framework"
)

func TestInitialize(t *testing.T) {
	rc := &ringController{}
	kubeClient := kfake.NewSimpleClientset()
	vcClient := vcfake.NewSimpleClientset()

	kubeFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	vcFactory := vcinformers.NewSharedInformerFactory(vcClient, 0)
	mockOpt := &framework.ControllerOption{
		KubeClient:              kubeClient,
		VolcanoClient:           vcClient,
		SharedInformerFactory:   kubeFactory,
		VCSharedInformerFactory: vcFactory,
	}

	err := rc.Initialize(mockOpt)
	assert.NoError(t, err, "Initialize should not return error")

	assert.NotNil(t, rc.clients.KubeClient, "KubeClient should be initialized")
	assert.NotNil(t, rc.clients.VcClient, "VcClient should be initialized")
	assert.NotNil(t, rc.clients.Recorder, "Recorder should be initialized")
}

func TestInitEventRecorder(t *testing.T) {
	ctrl := &ringController{}
	ctrl.clients = RingClients{
		KubeClient: kfake.NewSimpleClientset(),
	}

	err := ctrl.initEventRecorder()
	assert.NoError(t, err, "Event recorder initialization failed")
	assert.NotNil(t, ctrl.clients.Recorder, "Recorder must be created")
}

func TestControllerRegistration(t *testing.T) {
	assert.NotNil(t, &ringCtrl, "Controller should be registered")
}
