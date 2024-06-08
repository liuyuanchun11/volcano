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

package hyperjob

import (
	"flag"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	batchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	vcclient "volcano.sh/apis/pkg/client/clientset/versioned"
	"volcano.sh/volcano/pkg/controllers/framework"
)

var (
	scheme = runtime.NewScheme()
)

type hyperJobController struct {
	kubeClient kubernetes.Interface
	vcClient   vcclient.Interface
}

func init() {
	batchv1.AddToScheme(scheme)
	corev1.AddToScheme(scheme)

	framework.RegisterController(&hyperJobController{})
}

func (hjc *hyperJobController) Name() string {
	return "hyperJob-controller"
}

func (hjc *hyperJobController) Initialize(opt *framework.ControllerOption) error {
	klog.V(3).Infof("Initializing hyperJob-controller")
	defer klog.V(3).Infof("Initialized hyperJob-controller done")

	hjc.kubeClient = opt.KubeClient
	hjc.vcClient = opt.VolcanoClient

	var qps float64
	var burst int
	flag.Float64Var(&qps, "kube-api-qps", 500, "Maximum QPS to use while talking with Kubernetes API")
	flag.IntVar(&burst, "kube-api-burst", 500, "Maximum burst for throttle while talking with Kubernetes API")

	kubeConfig := ctrl.GetConfigOrDie()
	kubeConfig.QPS = float32(qps)
	kubeConfig.Burst = burst

	mgr, err := ctrl.NewManager(kubeConfig, ctrl.Options{
		Scheme:         scheme,
		LeaderElection: false,
	})
	if err != nil {
		klog.Errorf("Failed to create hyperJob controller err: %v", err)
		return err
	}

	ctx := ctrl.SetupSignalHandler()
	if err := SetupHyperJobIndexes(ctx, mgr.GetFieldIndexer()); err != nil {
		klog.Errorf("Failed to setup hyperjob reconciler indexes")
		return err
	}

	controller := NewHyperJobReconciler(mgr.GetClient(), mgr.GetScheme(), mgr.GetEventRecorderFor("HyperJob"))
	controller.KubeClient = hjc.kubeClient
	controller.VcClient = hjc.vcClient

	if err := controller.SetupWithManager(mgr); err != nil {
		klog.Errorf("Failed to setup hyperjob reconciler controller, err: %v", err)
		return err
	}

	go func() {
		defer utilruntime.HandleCrash()
		if err = mgr.Start(ctx); err != nil {
			klog.Errorf("Failed to start hyperJob controller manager: %v", err)
			os.Exit(1)
		}
	}()

	return nil
}

func (hjc *hyperJobController) Run(stopCh <-chan struct{}) {}
