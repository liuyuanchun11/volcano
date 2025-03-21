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
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func (rc *RankTblController) InitJobInformer() error {
	rc.jobInformer = rc.vcSharedInformerFactory.Batch().V1alpha1().Jobs().Informer()
	rc.jobLister = rc.vcSharedInformerFactory.Batch().V1alpha1().Jobs().Lister()
	if _, err := rc.jobInformer.AddEventHandler(rc.newJobHandler()); err != nil {
		return err
	}

	return nil
}

func (rc *RankTblController) GetJobByEvent(event SyncEvent) (*vcbatchv1.Job, error) {
	job, err := rc.jobLister.Jobs(event.Namespace).Get(event.Name)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (rc *RankTblController) CheckJobValid(job *vcbatchv1.Job) bool {
	if job.DeletionTimestamp != nil {
		klog.V(4).Infof("Job %v is being deleted", klog.KObj(job))
		return false
	}

	phase := job.Status.State.Phase
	if phase == vcbatchv1.Completed || phase == vcbatchv1.Terminated ||
		phase == vcbatchv1.Failed || phase == vcbatchv1.Aborted {
		klog.V(4).Infof("Job %v has entered the final state %s", klog.KObj(job), phase)
		return false
	}

	return true
}

func (rc *RankTblController) newJobHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { rc.handleJobEvent(obj) },
		UpdateFunc: func(oldObj, newObj interface{}) { rc.handleJobEvent(newObj) },
		DeleteFunc: func(obj interface{}) { rc.handleJobEvent(obj) },
	}
}

func (rc *RankTblController) handleJobEvent(obj interface{}) {
	var job *vcbatchv1.Job

	switch t := obj.(type) {
	case *vcbatchv1.Job:
		job = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		job, ok = t.Obj.(*vcbatchv1.Job)
		if !ok {
			klog.Errorf("Couldn't get job from tombstone %#v", obj)
			return
		}
	default:
		klog.Errorf("Expected job but got %T", obj)
		return
	}

	if !checkJobCare(job) {
		klog.V(6).Infof("Ignore job %v event", klog.KObj(job))
		return
	}

	ownerType, ownerName := getOwnerByJob(job)

	event := SyncEvent{
		JobType:   ownerType,
		Name:      ownerName,
		Namespace: job.Namespace,
	}
	rc.queue.AddAfter(event, workQueueAddDelay)
	klog.V(4).Infof("Received job %v event, add SyncEvent %s", klog.KObj(job), event.GetKey())
}

func checkJobCare(job *vcbatchv1.Job) bool {
	if value, exist := job.Labels[cutil.Label1980Key]; !exist || value != cutil.Label1980Value {
		klog.V(6).Infof("Ignore job %v event, because it doesn't cantain %s label", klog.KObj(job), cutil.Label1980Key)
		return false
	}

	return true
}

func getOwnerByJob(job *vcbatchv1.Job) (string, string) {
	if hjName, exist := job.Annotations[vcbatchv1.HyperJobNameKey]; exist {
		return SyncTypeHyperJob, hjName
	}
	return SyncTypeJob, job.Name
}
