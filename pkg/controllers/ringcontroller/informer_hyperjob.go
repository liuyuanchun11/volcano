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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func (rc *RankTblController) InitHyperJobInformer() error {
	rc.hyperJobInformer = rc.vcSharedInformerFactory.Batch().V1alpha1().HyperJobs().Informer()
	rc.hyperJobLister = rc.vcSharedInformerFactory.Batch().V1alpha1().HyperJobs().Lister()
	if _, err := rc.hyperJobInformer.AddEventHandler(rc.newHyperJobHandler()); err != nil {
		return err
	}

	return nil
}

func (rc *RankTblController) GetHyperJobByEvent(event SyncEvent) (*vcbatchv1.HyperJob, error) {
	hyperJob, err := rc.hyperJobLister.HyperJobs(event.Namespace).Get(event.Name)
	if err != nil {
		return nil, err
	}
	return hyperJob, nil
}

func (rc *RankTblController) CheckHyperJobValid(hyperJob *vcbatchv1.HyperJob) bool {
	if hyperJob.DeletionTimestamp != nil {
		klog.V(4).Infof("HyperJob %v is being deleted", klog.KObj(hyperJob))
		return false
	}

	for _, con := range hyperJob.Status.Conditions {
		if (con.Type == string(vcbatchv1.HyperJobFailed) || con.Type == string(vcbatchv1.HyperJobCompleted)) &&
			con.Status == metav1.ConditionTrue {
			klog.V(4).Infof("HyperJob is in %s condition", con.Type)
			return false
		}
	}

	return true
}

func (rc *RankTblController) newHyperJobHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { rc.handleHyperJobEvent(obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { rc.handleHyperJobEvent(newObj) },
		DeleteFunc: func(obj interface{}) { rc.handleHyperJobEvent(obj) },
	}
}

func (rc *RankTblController) handleHyperJobEvent(obj interface{}) {
	var hyperJob *vcbatchv1.HyperJob

	switch t := obj.(type) {
	case *vcbatchv1.HyperJob:
		hyperJob = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		hyperJob, ok = t.Obj.(*vcbatchv1.HyperJob)
		if !ok {
			klog.Errorf("Couldn't get hyperJob from tombstone %#v", obj)
			return
		}
	default:
		klog.Errorf("Expected hyperJob but got %T", obj)
		return
	}

	if !checkHyperJobCare(hyperJob) {
		klog.V(6).Infof("Ignore hyperJob %v event", klog.KObj(hyperJob))
		return
	}

	ownerType, ownerName := getOwnerByHyperJob(hyperJob)

	event := SyncEvent{
		JobType:   ownerType,
		Name:      ownerName,
		Namespace: hyperJob.Namespace,
	}
	rc.queue.AddAfter(event, workQueueAddDelay)
	klog.V(4).Infof("Received hyperJob %v event, add SyncEvent %s", klog.KObj(hyperJob), event.GetKey())
}

func checkHyperJobCare(hyperJob *vcbatchv1.HyperJob) bool {
	if value, exist := hyperJob.Labels[cutil.Label1980Key]; !exist || value != cutil.Label1980Value {
		klog.V(6).Infof("Ignore job %v event, because it doesn't cantain %s label", klog.KObj(hyperJob), cutil.Label1980Key)
		return false
	}

	return true
}

func getOwnerByHyperJob(hyperJob *vcbatchv1.HyperJob) (string, string) {
	return SyncTypeHyperJob, hyperJob.Name
}
