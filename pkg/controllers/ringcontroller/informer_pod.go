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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/apis/helpers"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func (rc *RankTblController) InitPodInformer() error {
	rc.podInformer = rc.kubeSharedInformerFactory.Core().V1().Pods().Informer()
	rc.podLister = rc.kubeSharedInformerFactory.Core().V1().Pods().Lister()

	if _, err := rc.podInformer.AddEventHandler(rc.newPodHandler()); err != nil {
		return err
	}
	return nil
}

func (rc *RankTblController) newPodHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { rc.handlePodEvent(obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { rc.handlePodEvent(newObj) },
		DeleteFunc: func(obj interface{}) { rc.handlePodEvent(obj) },
	}
}

func (rc *RankTblController) handlePodEvent(obj interface{}) {
	var pod *corev1.Pod

	switch t := obj.(type) {
	case *corev1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*corev1.Pod)
		if !ok {
			klog.Errorf("Couldn't get pod from tombstone %#v", obj)
			return
		}
	default:
		klog.Errorf("Expected pod but got %T", obj)
		return
	}

	if !checkPodCare(pod) {
		klog.V(6).Infof("Ignore pod %v event", klog.KObj(pod))
		return
	}

	ownerType, ownerName, err := getOwnerByPod(pod)
	if err != nil {
		klog.Errorf("Failed to get owner by pod %s: %v", klog.KObj(pod), err)
		return
	}

	event := SyncEvent{
		JobType:   ownerType,
		Name:      ownerName,
		Namespace: pod.Namespace,
	}
	rc.queue.AddAfter(event, workQueueAddDelay)
	klog.V(4).Infof("Received pod %v event, add SyncEvent %s", klog.KObj(pod), event.GetKey())
}

func checkPodCare(pod *corev1.Pod) bool {
	if !IsControlledBy(pod, helpers.JobKind) {
		klog.V(6).Infof("Ignore pod %v event, because it's not controller by vcjob/hyperjob", klog.KObj(pod))
		return false
	}

	if value, exist := pod.Labels[cutil.Label1980Key]; !exist || value != cutil.Label1980Value {
		klog.V(6).Infof("Ignore pod %v event, because it doesn't cantain %s label", klog.KObj(pod), cutil.Label1980Key)
		return false
	}

	return true
}

func IsControlledBy(obj metav1.Object, gvk schema.GroupVersionKind) bool {
	controllerRef := metav1.GetControllerOf(obj)
	if controllerRef == nil {
		return false
	}
	if controllerRef.APIVersion == gvk.GroupVersion().String() && controllerRef.Kind == gvk.Kind {
		return true
	}
	return false
}

func getOwnerByPod(pod *corev1.Pod) (string, string, error) {
	var ownerType string
	var ownerName string
	if hjName, exist := pod.Annotations[vcbatchv1.HyperJobNameKey]; exist {
		ownerType = SyncTypeHyperJob
		ownerName = hjName
	} else if jobName, exist := pod.Annotations[vcbatchv1.JobNameKey]; exist {
		ownerType = SyncTypeJob
		ownerName = jobName
	} else {
		return "", "", fmt.Errorf("failed to get job or hyperjob name")
	}
	return ownerType, ownerName, nil
}
