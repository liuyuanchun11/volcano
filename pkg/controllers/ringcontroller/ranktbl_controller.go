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
	"time"

	"golang.org/x/time/rate"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	vcinformers "volcano.sh/apis/pkg/client/informers/externalversions"
	vclisters "volcano.sh/apis/pkg/client/listers/batch/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/framework"
)

const (
	workerCount = 1

	workQueueBaseDelay = 10 * time.Millisecond
	workQueueMaxDelay  = 10 * time.Second
	workQueueRateLimit = 50
	workQueueBurst     = 500
	workQueueAddDelay  = 10 * time.Millisecond
)

type RankTblController struct {
	clients RingClients

	kubeSharedInformerFactory informers.SharedInformerFactory
	vcSharedInformerFactory   vcinformers.SharedInformerFactory

	jobInformer       cache.SharedIndexInformer
	jobLister         vclisters.JobLister
	hyperJobInformer  cache.SharedIndexInformer
	hyperJobLister    vclisters.HyperJobLister
	podInformer       cache.SharedIndexInformer
	podLister         listerv1.PodLister
	configmapInformer cache.SharedIndexInformer
	configmapLister   listerv1.ConfigMapLister

	queue workqueue.TypedRateLimitingInterface[SyncEvent]

	npuDevSyncerMap NPUDevSyncerMap
}

func (rc *RankTblController) Initialize(clients RingClients, opt *framework.ControllerOption) error {
	rc.clients = clients
	rc.kubeSharedInformerFactory = opt.SharedInformerFactory
	rc.vcSharedInformerFactory = opt.VCSharedInformerFactory

	if err := rc.initWorkQueue(); err != nil {
		return fmt.Errorf("init workQueue failed: %v", err)
	}

	if err := rc.InitPodInformer(); err != nil {
		return fmt.Errorf("init pod informer failed: %v", err)
	}
	if err := rc.InitJobInformer(); err != nil {
		return fmt.Errorf("init job informer failed: %v", err)
	}
	if err := rc.InitHyperJobInformer(); err != nil {
		return fmt.Errorf("init hyperJob informer failed: %v", err)
	}
	if err := rc.InitConfigmapInformer(); err != nil {
		return fmt.Errorf("init configmap informer failed: %v", err)
	}
	if err := rc.npuDevSyncerMap.Initialize(rc.podLister); err != nil {
		return fmt.Errorf("init npu device syncer map failed: %v", err)
	}
	klog.V(3).Infof("Initialized ranktable controller successfully")
	return nil
}

func (rc *RankTblController) Run(stopCh <-chan struct{}) {
	defer runtime.HandleCrash()

	klog.V(2).Infof("Starting rankTbl controller")
	go rc.jobInformer.Run(stopCh)
	go rc.hyperJobInformer.Run(stopCh)
	go rc.podInformer.Run(stopCh)
	go rc.configmapInformer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, rc.jobInformer.HasSynced, rc.hyperJobInformer.HasSynced,
		rc.podInformer.HasSynced, rc.configmapInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("cache sync failed"))
		return
	}

	klog.V(2).Infof("RankTbl controller cache synced. Starting %d workers", workerCount)
	// 暂时不考虑并发
	go wait.Until(rc.runWorker, time.Second, stopCh)

	<-stopCh
	klog.V(2).Infof("Shutting down rankTbl controller's %d workers", workerCount)
}

func (rc *RankTblController) initWorkQueue() error {
	rc.queue = workqueue.NewTypedRateLimitingQueue[SyncEvent](
		workqueue.NewTypedMaxOfRateLimiter(
			workqueue.NewTypedItemExponentialFailureRateLimiter[SyncEvent](workQueueBaseDelay, workQueueMaxDelay),
			&workqueue.TypedBucketRateLimiter[SyncEvent]{Limiter: rate.NewLimiter(rate.Limit(workQueueRateLimit), workQueueBurst)},
		),
	)
	return nil
}

func (rc *RankTblController) runWorker() {
	for rc.processNextItem() {
	}
}

func (rc *RankTblController) processNextItem() bool {
	event, quit := rc.queue.Get()
	if quit {
		return false
	}
	defer rc.queue.Done(event)

	klog.V(4).Infof("Handle event %s begin", event.GetKey())
	defer klog.V(4).Infof("Handle event %s end", event.GetKey())
	if err := rc.syncHandler(event); err != nil {
		rc.queue.AddRateLimited(event)
		klog.V(4).Infof("Failed to handle sync event %s, err: %v", event.GetKey(), err)
	} else {
		rc.queue.Forget(event)
	}

	return true
}

func (rc *RankTblController) syncHandler(event SyncEvent) error {
	klog.V(3).Infof("Sync event %s ranktable begin", event.GetKey())
	defer klog.V(3).Infof("Sync event %s ranktable end", event.GetKey())

	nds, err := rc.getHandlerSyncer(event)
	if err != nil {
		return err
	}
	if nds == nil {
		return nil
	}

	var changeList []*ChangeEvent
	defer func() {
		rc.RecordChangeEvents(nds.Spec, changeList)
	}()

	podEvent := nds.SyncPodNpuDevs()
	if podEvent != nil {
		changeList = append(changeList, podEvent)
	}
	newRanktable, torList := nds.GenerateRankTbl()

	oldRanktable, err := rc.GetOldRanktable(nds.Spec)
	if err != nil {
		klog.V(3).Infof("Get syncer %s current ranktable failed, err: %v", nds.Name, err)
		return err
	}

	newRanktable, needUpdate, needRearrange, rankTblEvent := CheckRankTblUpdate(oldRanktable, newRanktable)
	if rankTblEvent != nil {
		changeList = append(changeList, rankTblEvent)
	}
	if !needUpdate {
		klog.V(4).Infof("Syncer %s no ranktable need to update", nds.Name)
		return nil
	}

	if needRearrange {
		rearranged, rearrangeEvent := rc.RanktableRearrange(nds, oldRanktable, newRanktable)
		if rearrangeEvent != nil {
			changeList = append(changeList, rearrangeEvent)
		}
		newRanktable = rearranged
	}

	cmChangeList, err := rc.BindRankTbl2Pods(nds.Spec, newRanktable, torList)
	if len(cmChangeList) != 0 {
		changeList = append(changeList, cmChangeList...)
	}
	if err != nil {
		klog.V(3).Infof("Bind ranktable %s failed, err: %v", nds.Name, err)
		return err
	}
	return nil
}

func (rc *RankTblController) getHandlerSyncer(event SyncEvent) (*NPUDeviceSyncer, error) {
	owner, err := rc.getSyncOwner(event)
	if err != nil {
		klog.V(3).Infof("Failed to get sync owner %s: %v", event.GetKey(), err)
		rc.killHandle(event)
		return nil, nil
	}

	if !rc.checkSyncOwnerValid(owner) {
		klog.V(3).Infof("Owner %s is being deleted or has entered its final state", event.GetKey())
		rc.killHandle(event)
		return nil, nil
	}

	var changeList []*ChangeEvent
	var nds *NPUDeviceSyncer
	nds = rc.npuDevSyncerMap.GetNPUDevSyncer(event)
	if nds == nil {
		klog.V(3).Infof("Could not find npu device syncer for %s", event.GetKey())
		nds, err = rc.npuDevSyncerMap.AddNPUDevSyncer(owner)
		if err != nil {
			klog.V(3).Infof("Failed to add npu device syncer for %s: %v", event.GetKey(), err)
			return nil, fmt.Errorf("add %s npu device syncer failed, err: %v", event.GetKey(), err)
		}
	} else {
		changeEvent := nds.UpdateNPUDevSyncer(owner)
		if changeEvent != nil {
			changeList = append(changeList, changeEvent)
		}
	}

	rc.RecordChangeEvents(nds.Spec, changeList)
	return nds, nil
}

func (rc *RankTblController) getSyncOwner(event SyncEvent) (interface{}, error) {
	switch event.JobType {
	case SyncTypeJob:
		return rc.GetJobByEvent(event)
	case SyncTypeHyperJob:
		return rc.GetHyperJobByEvent(event)
	default:
		return nil, fmt.Errorf("not support event %v", event)
	}
}

func (rc *RankTblController) checkSyncOwnerValid(owner interface{}) bool {
	switch job := owner.(type) {
	case *vcbatchv1.Job:
		return rc.CheckJobValid(job)
	case *vcbatchv1.HyperJob:
		return rc.CheckHyperJobValid(job)
	default:
		klog.V(3).Infof("Not support owner %v", owner)
		return false
	}
}

func (rc *RankTblController) killHandle(event SyncEvent) {
	nds := rc.npuDevSyncerMap.GetNPUDevSyncer(event)
	if nds == nil {
		return
	}

	rc.RanktableRearrangeRelease(nds)
	rc.npuDevSyncerMap.DeleteNPUDevSyncer(event)
	klog.V(4).Infof("Deleted npu device syncer for %s", event.GetKey())
}
