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
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	vcinformers "volcano.sh/apis/pkg/client/informers/externalversions"
	"volcano.sh/volcano/pkg/controllers/framework"
)

type ringController struct {
	clients RingClients

	kubeSharedInformerFactory informers.SharedInformerFactory
	vcSharedInformerFactory   vcinformers.SharedInformerFactory

	rankTblCtrl RankTblController
}

var ringCtrl ringController

func init() {
	if err := framework.RegisterController(&ringCtrl); err != nil {
		klog.Errorf("Failed to initial ring controller, err: %v", err)
	}
}

func (rc *ringController) Name() string {
	return "ring-controller"
}

func (rc *ringController) Initialize(opt *framework.ControllerOption) error {
	rc.clients.KubeClient = opt.KubeClient
	rc.clients.VcClient = opt.VolcanoClient

	rc.kubeSharedInformerFactory = opt.SharedInformerFactory
	rc.vcSharedInformerFactory = opt.VCSharedInformerFactory

	err := rc.initEventRecorder()
	if err != nil {
		return fmt.Errorf("initialize event recorder err: %v", err)
	}

	if err := rc.rankTblCtrl.Initialize(rc.clients, opt); err != nil {
		return fmt.Errorf("initialize rankTbl controller err: %v", err)
	}
	klog.V(2).Infof("Initialized ring controller successfully")
	return nil
}

func (rc *ringController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	rc.rankTblCtrl.Run(stopCh)

	<-stopCh
}

func (rc *ringController) initEventRecorder() error {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: rc.clients.KubeClient.CoreV1().Events("")})

	scheme := runtime.NewScheme()
	err := vcbatchv1.AddToScheme(scheme)
	if err != nil {
		return fmt.Errorf("add vcbatchv1 to scheme err: %v", err)
	}
	err = v1.AddToScheme(scheme)
	if err != nil {
		return fmt.Errorf("add v1 to scheme err: %v", err)
	}

	rc.clients.Recorder = eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "ring-controller"})
	return nil
}
