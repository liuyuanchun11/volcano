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
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	vcclient "volcano.sh/apis/pkg/client/clientset/versioned"
)

type HyperJobReconciler struct {
	client.Client
	KubeClient kubernetes.Interface
	VcClient   vcclient.Interface
	Scheme     *runtime.Scheme
	Record     record.EventRecorder
}

type vcJobs struct {
	active    []*vcbatch.Job
	succeeded []*vcbatch.Job
	failed    []*vcbatch.Job
	pending   []*vcbatch.Job

	delete []*vcbatch.Job
}

type ensureConditionOpts struct {
	hyperJob         *vcbatch.HyperJob
	eventType        string
	forceFalseUpdate bool
	condition        metav1.Condition
}

const hyperJobFinalizer = "hyperjob.batch.volcano.sh/finalizer"

func NewHyperJobReconciler(client client.Client, scheme *runtime.Scheme, record record.EventRecorder) *HyperJobReconciler {
	return &HyperJobReconciler{Client: client, Scheme: scheme, Record: record}
}

func SetupHyperJobIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &vcbatch.Job{}, JobOwnerKey, func(obj client.Object) []string {
		o, ok := obj.(*vcbatch.Job)
		if !ok {
			return nil
		}
		owner := metav1.GetControllerOf(o)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != vcbatch.SchemeGroupVersion.String() || owner.Kind != "HyperJob" {
			return nil
		}
		return []string{owner.Name}
	})
}

func (hjr *HyperJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&vcbatch.HyperJob{}).
		Owns(&vcbatch.Job{}).
		Owns(&corev1.Service{}).
		Complete(hjr)
}

func (hjr *HyperJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var hyperJob vcbatch.HyperJob
	if err := hjr.Get(ctx, req.NamespacedName, &hyperJob); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	klog.V(2).Infof("The hyperjob Reconciler starts synchronizing %s/%s", hyperJob.Namespace, hyperJob.Name)
	defer klog.V(2).Infof("HyperJob %s/%s has been synced", hyperJob.Namespace, hyperJob.Name)

	if hyperJob.DeletionTimestamp != nil {
		if err := hjr.cleanupHyperJob(&hyperJob); err != nil {
			klog.Errorf("Cleanup hyperJob err: %v", err)
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	ownedJobs, err := hjr.getVcJobs(ctx, &hyperJob)
	if err != nil {
		klog.Errorf("Get vcjobs owned by %s/%s failed, err: %v", hyperJob.Namespace, hyperJob.Name, err)
		return ctrl.Result{}, err
	}

	if err := hjr.initiateHyperJob(&hyperJob, ownedJobs); err != nil {
		klog.Errorf("Initiate hyperJob err: %v", err)
		return ctrl.Result{}, err
	}

	vcJobsStatus := hjr.calculateVcJobStatus(&hyperJob, ownedJobs)
	if err := hjr.updateVcJobsStatus(ctx, &hyperJob, vcJobsStatus); err != nil {
		klog.Errorf("Update replication jobs status err: %v", err)
		return ctrl.Result{}, err
	}

	// If hyperJob is failed or completed, cleanup active/pending vcjobs
	if hjr.isHyperJobFinished(&hyperJob) {
		if err := hjr.cleanupUnfinishedJobs(ctx, ownedJobs); err != nil {
			klog.Errorf("Cleanup unfinished jobs failed: %v", err)
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if err := hjr.deleteVcJobs(ctx, ownedJobs.delete); err != nil {
		klog.Errorf("Delete vcjobs failed: %v", err)
		return ctrl.Result{}, err
	}

	if len(ownedJobs.failed) > 0 {
		if err := hjr.executeFailurePolicy(ctx, &hyperJob, ownedJobs); err != nil {
			klog.Errorf("Execute failure policy failed: %v", err)
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if len(ownedJobs.succeeded) > 0 {
		completed, err := hjr.executeSuccessPolicy(ctx, &hyperJob, ownedJobs)
		if err != nil {
			klog.Errorf("Execute success policy failed: %v", err)
			return ctrl.Result{}, err
		}
		if completed {
			return ctrl.Result{}, nil
		}
	}

	if err := hjr.createHeadlessSvcIfNotExist(ctx, &hyperJob); err != nil {
		klog.Errorf("Create headless service failed, err: %v", err)
		return ctrl.Result{}, err
	}

	if err := hjr.syncVcJobs(ctx, &hyperJob, ownedJobs); err != nil {
		klog.Errorf("Create vcjobs failed: %v", err)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (hjr *HyperJobReconciler) getVcJobs(ctx context.Context, hyperJob *vcbatch.HyperJob) (*vcJobs, error) {
	var vcJobList vcbatch.JobList
	if err := hjr.List(ctx, &vcJobList, client.InNamespace(hyperJob.Namespace), client.MatchingFields{JobOwnerKey: hyperJob.Name}); err != nil {
		return nil, err
	}

	klog.V(4).Infof("List hyperjob %s/%s owned %d jobs", hyperJob.Namespace, hyperJob.Name, len(vcJobList.Items))

	ownedJobs := vcJobs{}
	for i, job := range vcJobList.Items {
		klog.V(4).Infof("Get owned job %s phase %s", job.Name, job.Status.State.Phase)
		switch job.Status.State.Phase {
		case vcbatch.Completed:
			ownedJobs.succeeded = append(ownedJobs.succeeded, &vcJobList.Items[i])
		case vcbatch.Failed, vcbatch.Terminating, vcbatch.Terminated, vcbatch.Aborting, vcbatch.Aborted:
			ownedJobs.failed = append(ownedJobs.failed, &vcJobList.Items[i])
		case vcbatch.Pending:
			ownedJobs.pending = append(ownedJobs.pending, &vcJobList.Items[i])
		case vcbatch.Running, vcbatch.Completing, vcbatch.Restarting:
			ownedJobs.active = append(ownedJobs.active, &vcJobList.Items[i])
		default:
			klog.V(4).Infof("Job %s phase %s is unexpected", job.Name, job.Status.State.Phase)
		}
	}
	return &ownedJobs, nil
}

func (hjr *HyperJobReconciler) calculateVcJobStatus(hyperJob *vcbatch.HyperJob, jobs *vcJobs) []vcbatch.ReplicatedJobStatus {
	jobsStatus := map[string]map[string]int32{}
	for _, replicateJobs := range hyperJob.Spec.ReplicatedJobs {
		jobsStatus[replicateJobs.Name] = map[string]int32{
			JobStatusReady:     0,
			JobStatusSucceeded: 0,
			JobStatusFailed:    0,
			JobStatusActive:    0,
			JobStatusPending:   0,
		}
	}

	for _, job := range jobs.active {
		if job.Labels == nil || (job.Labels != nil && job.Labels[vcbatch.HyperJobReplicatedJobNameKey] == "") {
			klog.V(2).Infof("Job %s missing replicatedJobName label", job.Name)
			continue
		}

		if job.Status.MinAvailable > 0 && job.Status.Succeeded >= job.Status.MinAvailable {
			jobsStatus[job.Labels[vcbatch.HyperJobReplicatedJobNameKey]][JobStatusReady]++
		} else {
			jobsStatus[job.Labels[vcbatch.HyperJobReplicatedJobNameKey]][JobStatusActive]++
		}
	}

	for _, job := range jobs.pending {
		jobsStatus[job.Labels[vcbatch.HyperJobReplicatedJobNameKey]][JobStatusPending]++
	}

	for _, job := range jobs.succeeded {
		jobsStatus[job.Labels[vcbatch.HyperJobReplicatedJobNameKey]][JobStatusSucceeded]++
	}

	for _, job := range jobs.failed {
		jobsStatus[job.Labels[vcbatch.HyperJobReplicatedJobNameKey]][JobStatusFailed]++
	}

	var vcJobsStatus []vcbatch.ReplicatedJobStatus
	for name, status := range jobsStatus {
		vcJobsStatus = append(vcJobsStatus, vcbatch.ReplicatedJobStatus{
			Name:               name,
			Ready:              status[JobStatusReady],
			Succeeded:          status[JobStatusSucceeded],
			Failed:             status[JobStatusFailed],
			Active:             status[JobStatusActive],
			Pending:            status[JobStatusPending],
			LastTransitionTime: metav1.Now(),
		})
	}
	return vcJobsStatus
}

func (hjr *HyperJobReconciler) initiateHyperJob(hyperJob *vcbatch.HyperJob, ownedJobs *vcJobs) error {
	klog.V(4).Infof("Initiate hyperjob %s/%s", hyperJob.Namespace, hyperJob.Name)
	if len(ownedJobs.active) > 0 || len(ownedJobs.succeeded) > 0 || len(ownedJobs.pending) > 0 ||
		len(ownedJobs.failed) > 0 || len(ownedJobs.delete) > 0 {
		// If the number of vcJobs in any state is greater than 0, the hyperJob has been initialized
		return nil
	}
	return hjr.pluginOnHyperJobAdd(hyperJob)
}

func (hjr *HyperJobReconciler) cleanupHyperJob(hyperJob *vcbatch.HyperJob) error {
	if controllerutil.ContainsFinalizer(hyperJob, hyperJobFinalizer) {
		controllerutil.RemoveFinalizer(hyperJob, hyperJobFinalizer)
		if err := hjr.Client.Update(context.TODO(), hyperJob); err != nil {
			klog.Errorf("Remove hyperjob %s/%s finalizer err: %v", hyperJob.Namespace, hyperJob.Name, err)
		}
	}

	klog.V(4).Infof("Cleanup hyperjob %s/%s", hyperJob.Namespace, hyperJob.Name)
	return hjr.pluginOnHyperJobDelete(hyperJob)
}

func (hjr *HyperJobReconciler) syncVcJobs(ctx context.Context, hyperJob *vcbatch.HyperJob, ownedJobs *vcJobs) error {
	klog.V(5).Infof("Start syncing jobs")
	defer klog.V(5).Infof("Sync jobs is complete")

	var allErrs error
	jobs := generateExistJobs(ownedJobs)

	toCreateJobs := make([]*vcbatch.Job, 0)
	toDeleteJobs := make([]*vcbatch.Job, 0)

	for _, replicateJobs := range hyperJob.Spec.ReplicatedJobs {
		for jobIdx := 0; jobIdx < int(replicateJobs.Replicas); jobIdx++ {
			jobName := GetJobName(hyperJob.Name, replicateJobs.Name, jobIdx)
			_, found := jobs[jobName]
			if !found {
				newJob := constructJob(hyperJob, &replicateJobs, jobIdx)
				err := hjr.pluginOnJobCreate(hyperJob, newJob)
				if err != nil {
					allErrs = errors.Join(allErrs, err)
					continue
				}

				toCreateJobs = append(toCreateJobs, newJob)
			} else {
				// Remove the jobs that match the replicateJobs from map[string]*vcbatch.Job.
				delete(jobs, jobName)
			}
		}
	}

	// Delete the jobs that don`t match the replicateJobs due to scale-in.
	for _, job := range jobs {
		toDeleteJobs = append(toDeleteJobs, job)
	}

	if len(toCreateJobs) != 0 {
		createErrs := hjr.createVcJobs(ctx, hyperJob, toCreateJobs)
		allErrs = errors.Join(allErrs, createErrs)
	}

	if len(toDeleteJobs) != 0 {
		deleteErrs := hjr.deleteVcJobs(ctx, toDeleteJobs)
		allErrs = errors.Join(allErrs, deleteErrs)
	}

	return allErrs
}

func (hjr *HyperJobReconciler) createVcJobs(ctx context.Context, hyperJob *vcbatch.HyperJob, jobCreate []*vcbatch.Job) error {
	lock := &sync.Mutex{}
	var finalErrs []error

	workqueue.ParallelizeUntil(ctx, MaxParallelism, len(jobCreate), func(i int) {
		job := jobCreate[i]

		klog.V(4).Infof("Creating job %s", job.Name)
		if err := ctrl.SetControllerReference(hyperJob, job, hjr.Scheme); err != nil {
			lock.Lock()
			defer lock.Unlock()
			finalErrs = append(finalErrs, err)
			return
		}

		if err := hjr.Create(ctx, job); client.IgnoreAlreadyExists(err) != nil {
			lock.Lock()
			defer lock.Unlock()
			klog.V(2).Infof("Failed to create job %s, err: %v", job.Name, err)
			finalErrs = append(finalErrs, fmt.Errorf("job %s creation failed, err: %v", job.Name, err))
			return
		}
		klog.V(2).Infof("Successfully created job %s", job.Name)
	})

	allErrs := errors.Join(finalErrs...)
	if allErrs != nil {
		hjr.Record.Eventf(hyperJob, corev1.EventTypeWarning, JobCreationFailedReason, allErrs.Error())
	}

	return allErrs
}

func (hjr *HyperJobReconciler) deleteVcJobs(ctx context.Context, jobsDelete []*vcbatch.Job) error {
	klog.V(5).Infof("Start deleting jobs")
	defer klog.V(5).Infof("Deletion jobs is complete")

	lock := &sync.Mutex{}
	var finalErrs []error
	workqueue.ParallelizeUntil(ctx, MaxParallelism, len(jobsDelete), func(i int) {
		job := jobsDelete[i]

		klog.V(4).Infof("Deleting job %s", job.Name)
		if job.DeletionTimestamp != nil {
			klog.V(2).Infof("Job %s is already being deleted", job.Name)
			return
		}
		foregroundPolicy := metav1.DeletePropagationForeground
		if err := hjr.Delete(ctx, job, &client.DeleteOptions{PropagationPolicy: &foregroundPolicy}); client.IgnoreNotFound(err) != nil {
			lock.Lock()
			defer lock.Unlock()
			klog.Error(err, fmt.Sprintf("Failed to delete job %s, err: %v", job.Name, err))
			finalErrs = append(finalErrs, err)
			return
		}
		klog.V(2).Infof("Successfully delete job %s", job.Name)
	})

	return errors.Join(finalErrs...)
}

func (hjr *HyperJobReconciler) executeFailurePolicy(ctx context.Context, hyperJob *vcbatch.HyperJob, ownedJobs *vcJobs) error {
	// Default policy
	// HyperJob will set failed for any job state is failed
	firstFailedJob := hjr.getFirstFailedJob(ownedJobs.failed)
	if firstFailedJob == nil {
		klog.V(4).Infof("Failed to get first failed job")
		return hjr.setHyperJobFail(ctx, hyperJob, FailedJobsReason, FailedJobsMessage)
	}
	message := fmt.Sprintf("%s, first failed job: %s", FailedJobsMessage, firstFailedJob.Name)
	return hjr.setHyperJobFail(ctx, hyperJob, FailedJobsReason, message)
}

func (hjr *HyperJobReconciler) executeSuccessPolicy(ctx context.Context, hyperJob *vcbatch.HyperJob, ownedJobs *vcJobs) (bool, error) {
	// Default policy
	// HyperJob will set completed for all jobs state are completed
	replicasNum := 0
	for _, replicatedJobs := range hyperJob.Spec.ReplicatedJobs {
		replicasNum += int(replicatedJobs.Replicas)
	}
	if len(ownedJobs.succeeded) >= replicasNum {
		err := hjr.setHyperJobCompleted(ctx, hyperJob, AllJobsCompletedReason, AllJobsCompletedMessage)
		if err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (hjr *HyperJobReconciler) createHeadlessSvcIfNotExist(ctx context.Context, hyperJob *vcbatch.HyperJob) error {
	var headlessSvc corev1.Service
	if err := hjr.Get(ctx, types.NamespacedName{Name: hyperJob.Name, Namespace: hyperJob.Namespace}, &headlessSvc); err != nil {
		if !apierrors.IsNotFound(err) {
			klog.V(3).Infof("Failed to get headless service for hyperJob %s/%s, err: %v",
				hyperJob.Namespace, hyperJob.Name, err)
			return err
		}

		headlessSvc := corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      hyperJob.Name,
				Namespace: hyperJob.Namespace,
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: "None",
				Selector: map[string]string{
					vcbatch.HyperJobNameKey:      hyperJob.Name,
					vcbatch.HyperJobNamespaceKey: hyperJob.Namespace,
				},
			},
		}

		if err := ctrl.SetControllerReference(hyperJob, &headlessSvc, hjr.Scheme); err != nil {
			return err
		}

		if err := hjr.Create(ctx, &headlessSvc); err != nil {
			return err
		}
		klog.V(2).Infof("Successfully created headless service %s/%s", headlessSvc.Namespace, headlessSvc.Name)
	}
	return nil
}

func (hjr *HyperJobReconciler) updateVcJobsStatus(ctx context.Context, hyperJob *vcbatch.HyperJob,
	replicatedJobStatus []vcbatch.ReplicatedJobStatus) error {
	if apiequality.Semantic.DeepEqual(hyperJob.Status.ReplicatedJobsStatus, replicatedJobStatus) {
		return nil
	}
	hyperJob.Status.ReplicatedJobsStatus = replicatedJobStatus
	klog.V(4).Infof("Update hyperJob %v replicatedJob status: %v",
		klog.KObj(hyperJob), hyperJob.Status.ReplicatedJobsStatus)
	return hjr.Status().Update(ctx, hyperJob)
}

func (hjr *HyperJobReconciler) getFirstFailedJob(failedJobs []*vcbatch.Job) *vcbatch.Job {
	var (
		firstFailedJob   *vcbatch.Job
		firstFailureTime *metav1.Time
	)
	for _, job := range failedJobs {
		failureTime := hjr.getJobFailureTime(job)
		if failureTime != nil && (firstFailedJob == nil || failureTime.Before(firstFailureTime)) {
			firstFailedJob = job
			firstFailureTime = failureTime
		}
	}
	return firstFailedJob
}

func (hjr *HyperJobReconciler) getJobFailureTime(job *vcbatch.Job) *metav1.Time {
	if job == nil {
		return nil
	}
	for _, c := range job.Status.Conditions {
		if c.Status == vcbatch.Failed || c.Status == vcbatch.Terminated || c.Status == vcbatch.Aborted {
			return c.LastTransitionTime
		}
	}
	return nil
}

func (hjr *HyperJobReconciler) setHyperJobFail(ctx context.Context, hyperJob *vcbatch.HyperJob, reason, message string) error {
	return hjr.updateHyperJobCondition(ctx, ensureConditionOpts{
		hyperJob: hyperJob,
		condition: metav1.Condition{
			Type:    string(vcbatch.HyperJobFailed),
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		},
		eventType: corev1.EventTypeNormal,
	})
}

func (hjr *HyperJobReconciler) setHyperJobCompleted(ctx context.Context, hyperJob *vcbatch.HyperJob, reason, message string) error {
	return hjr.updateHyperJobCondition(ctx, ensureConditionOpts{
		hyperJob: hyperJob,
		condition: metav1.Condition{
			Type:    string(vcbatch.HyperJobCompleted),
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		},
		eventType: corev1.EventTypeNormal,
	})
}

func (hjr *HyperJobReconciler) updateHyperJobCondition(ctx context.Context, opts ensureConditionOpts) error {
	condition := opts.condition
	hyperJob := opts.hyperJob

	condition.LastTransitionTime = metav1.Now()
	for index, con := range hyperJob.Status.Conditions {
		if condition.Type == con.Type && condition.Status != con.Status {
			hyperJob.Status.Conditions[index] = condition
			return hjr.updateHyperJobStatus(ctx, opts)
		} else if condition.Type == con.Type && condition.Status == con.Status &&
			condition.Reason == con.Reason && condition.Message == con.Message {
			return nil
		}
	}

	if opts.forceFalseUpdate {
		hyperJob.Status.Conditions = append(hyperJob.Status.Conditions, condition)
		return hjr.updateHyperJobStatus(ctx, opts)
	}

	if condition.Status == metav1.ConditionTrue {
		hyperJob.Status.Conditions = append(hyperJob.Status.Conditions, condition)
		return hjr.updateHyperJobStatus(ctx, opts)
	}
	return nil
}

func (hjr *HyperJobReconciler) updateHyperJobStatus(ctx context.Context, opts ensureConditionOpts) error {
	if err := hjr.Status().Update(ctx, opts.hyperJob); err != nil {
		return err
	}
	hjr.Record.Eventf(opts.hyperJob, opts.eventType, opts.condition.Reason, opts.condition.Message)
	return nil
}

func (hjr *HyperJobReconciler) isHyperJobFinished(hyperJob *vcbatch.HyperJob) bool {
	for _, con := range hyperJob.Status.Conditions {
		if (con.Type == string(vcbatch.HyperJobFailed) || con.Type == string(vcbatch.HyperJobCompleted)) &&
			con.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func (hjr *HyperJobReconciler) cleanupUnfinishedJobs(ctx context.Context, jobs *vcJobs) error {
	jobsDelete := make([]*vcbatch.Job, 0)
	jobsDelete = append(jobsDelete, jobs.active...)
	jobsDelete = append(jobsDelete, jobs.pending...)
	return hjr.deleteVcJobs(ctx, jobsDelete)
}

func generateExistJobs(ownedJobs *vcJobs) map[string]*vcbatch.Job {
	existJobs := make(map[string]*vcbatch.Job)
	for _, job := range Concat(ownedJobs.active, ownedJobs.succeeded, ownedJobs.failed, ownedJobs.pending, ownedJobs.delete) {
		existJobs[job.Name] = job
	}
	return existJobs
}

func constructJob(hyperJob *vcbatch.HyperJob, rjob *vcbatch.ReplicatedJob, jobIdx int) *vcbatch.Job {
	job := &vcbatch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      CloneMap(hyperJob.Labels),
			Annotations: CloneMap(hyperJob.Annotations),
			Name:        GetJobName(hyperJob.Name, rjob.Name, jobIdx),
			Namespace:   hyperJob.Namespace,
		},
		Spec: *rjob.Template.DeepCopy(),
	}

	labelAndAnnotateObject(job, hyperJob, rjob, jobIdx)
	inheritPlugins(job, hyperJob)

	return job
}

func labelAndAnnotateObject(obj metav1.Object, hyperJob *vcbatch.HyperJob, rjob *vcbatch.ReplicatedJob, jobIdx int) {
	labels := CloneMap(obj.GetLabels())
	labels[vcbatch.HyperJobNameKey] = hyperJob.Name
	labels[vcbatch.HyperJobNamespaceKey] = hyperJob.Namespace
	labels[vcbatch.HyperJobReplicatedJobNameKey] = rjob.Name
	labels[vcbatch.HyperJobReplicatedJobIndexKey] = strconv.Itoa(jobIdx)

	annotations := CloneMap(obj.GetAnnotations())
	annotations[vcbatch.HyperJobNameKey] = hyperJob.Name
	annotations[vcbatch.HyperJobNamespaceKey] = hyperJob.Namespace
	annotations[vcbatch.HyperJobReplicatedJobNameKey] = rjob.Name
	annotations[vcbatch.HyperJobReplicatedJobIndexKey] = strconv.Itoa(jobIdx)
	annotations[vcbatch.HyperJobUIDKey] = string(hyperJob.UID)

	// all pods in job which created by hyperjob must be scheduled to the same hypernode
	annotations[vcbatch.HyperNodeAffinityAnnotation] = vcbatch.Required

	obj.SetLabels(labels)
	obj.SetAnnotations(annotations)
}

func inheritPlugins(job *vcbatch.Job, hyperJob *vcbatch.HyperJob) {
	if job.Spec.Plugins == nil {
		job.Spec.Plugins = make(map[string][]string)
	}
	for k, v := range hyperJob.Spec.Plugins {
		if _, exist := job.Spec.Plugins[k]; !exist {
			job.Spec.Plugins[k] = v
		}
	}
}
