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
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

const (
	npuResourceName = "huawei.com/ascend-1980"
)

const (
	MaxWorkerCount = 16
)

type NPUDeviceSyncer struct {
	sync.RWMutex

	Namespace string
	Name      string

	// 属于job或hyperjob
	OwnerRef SyncerOwnerRef
	Spec     RankTblSpec

	podLister listerv1.PodLister
	// job/hyperjob内所有请求了npu资源的pod npu信息
	podDeviceInfo map[string]PodDeviceInfo
}

type SyncerOwnerRef struct {
	Owner interface{}
}

type PodDeviceInfo struct {
	DevInfo   PodNpuInfo
	IsCached  bool
	SortIndex string
	JobName   string
}

type PodEventType string

const (
	EventDeviceCached   PodEventType = "DeviceCached"
	EventDeviceCleared  PodEventType = "DeviceCleared"
	EventDeviceModified PodEventType = "DeviceModified"
	EventDeviceParseErr PodEventType = "DeviceParseErr"
)

type PodEvent struct {
	EventType PodEventType
	PodName   string
}

func NewNPUDevSyncer(podLister listerv1.PodLister, owner interface{}) (*NPUDeviceSyncer, error) {
	switch job := owner.(type) {
	case *vcbatchv1.Job:
		return createNPUDevSyncerByJob(podLister, job)
	case *vcbatchv1.HyperJob:
		return createNPUDevSyncerByHyperJob(podLister, job)
	default:
		klog.V(3).Infof("Init NPU Dev syncer failed, only support Job/HyperJob")
	}
	return nil, fmt.Errorf("only support Job/HyperJob")
}

func createNPUDevSyncerByJob(podLister listerv1.PodLister, job *vcbatchv1.Job) (*NPUDeviceSyncer, error) {
	syncer := NPUDeviceSyncer{
		podLister: podLister,
		OwnerRef: SyncerOwnerRef{
			Owner: job,
		},
		Namespace: job.Namespace,
		Name:      GetSyncerName(SyncTypeJob, job.Namespace, job.Name),
	}

	if err := syncer.initNPUDevSyncerSpecByJob(job); err != nil {
		return nil, err
	}

	syncer.initPodDeviceInfoByJob(job)
	klog.V(3).Infof("Create npu device syncer %s success, spec %+v, pod device num: %d",
		syncer.Name, syncer.Spec, len(syncer.podDeviceInfo))
	klog.V(5).Infof("Syncer %s pod device: %+v", syncer.Name, syncer.podDeviceInfo)
	return &syncer, nil
}

func (nds *NPUDeviceSyncer) initNPUDevSyncerSpecByJob(job *vcbatchv1.Job) error {
	cmArgs, err := cutil.GetPluginArgsByJob(job)
	if err != nil {
		return fmt.Errorf("init spec failed, err: %v", err)
	}

	nds.Spec.Namespace = nds.Namespace
	nds.Spec.RankTblVersion = cmArgs.RankTableVersion
	// Job不需要生成SuperPodList
	nds.Spec.NeedSuperPodList = false
	// 1.3 开始统一支持dataVersion
	if nds.Spec.RankTblVersion == cutil.RankTblV10 || nds.Spec.RankTblVersion == cutil.RankTblV12 {
		nds.Spec.DataVersionEnable = false
	} else {
		nds.Spec.DataVersionEnable = true
	}
	nds.Spec.JobStartCmName = cutil.GetConfigmapName(job.Name)
	// 按需开启jobstart_hccl.json压缩
	nds.Spec.RankTblCompress = cmArgs.RankTableCompress
	// job默认生成TorList，可按需关闭
	if cmArgs.RankTableTorEnable {
		nds.Spec.NeedTorList = true
	} else {
		nds.Spec.NeedTorList = false
	}

	if nds.Spec.NeedTorList {
		nds.Spec.TorListCmName = cutil.GetTorListCmName(job.Name)
	}

	return nil
}

func (nds *NPUDeviceSyncer) initPodDeviceInfoByJob(job *vcbatchv1.Job) {
	nds.podDeviceInfo = make(map[string]PodDeviceInfo)
	for _, task := range job.Spec.Tasks {
		if !isTaskRequestNpu(task) {
			continue
		}
		for i := 0; i < int(task.Replicas); i++ {
			podName := GetPodName(job.Name, task.Name, i)
			nds.podDeviceInfo[podName] = PodDeviceInfo{
				DevInfo:  PodNpuInfo{},
				IsCached: false,
				// server_list按照pod索引排序
				SortIndex: fmt.Sprintf("%05d", i),
			}
		}
	}
}

func createNPUDevSyncerByHyperJob(podLister listerv1.PodLister, hyperJob *vcbatchv1.HyperJob) (*NPUDeviceSyncer, error) {
	syncer := NPUDeviceSyncer{
		podLister: podLister,
		OwnerRef: SyncerOwnerRef{
			Owner: hyperJob,
		},
		Namespace: hyperJob.Namespace,
		Name:      GetSyncerName(SyncTypeHyperJob, hyperJob.Namespace, hyperJob.Name),
	}

	if err := syncer.initNPUDevSyncerSpecByHyperJob(hyperJob); err != nil {
		return nil, err
	}

	syncer.initPodDeviceInfoByHyperJob(hyperJob)
	klog.V(3).Infof("Create npu device syncer %s success, spec %+v, pod device num: %d",
		syncer.Name, syncer.Spec, len(syncer.podDeviceInfo))
	klog.V(5).Infof("Syncer %s pod device: %+v", syncer.Name, syncer.podDeviceInfo)
	return &syncer, nil
}

func (nds *NPUDeviceSyncer) initNPUDevSyncerSpecByHyperJob(hyperJob *vcbatchv1.HyperJob) error {
	cmArgs, err := cutil.GetPluginArgsByHyperJob(hyperJob)
	if err != nil {
		return fmt.Errorf("init spec failed, err: %v", err)
	}

	nds.Spec.Namespace = nds.Namespace
	nds.Spec.RankTblVersion = cmArgs.RankTableVersion
	// hyperJob默认需要生成SuperPodList
	nds.Spec.NeedSuperPodList = true
	// 1.3 开始统一支持dataVersion
	if nds.Spec.RankTblVersion == cutil.RankTblV12 {
		nds.Spec.DataVersionEnable = false
	} else {
		nds.Spec.DataVersionEnable = true
	}
	nds.Spec.JobStartCmName = cutil.GetConfigmapName(hyperJob.Name)
	// 按需开启jobstart_hccl.json压缩
	nds.Spec.RankTblCompress = cmArgs.RankTableCompress
	// hyperJob不需要生成TorList
	nds.Spec.NeedTorList = false

	return nil
}

func (nds *NPUDeviceSyncer) initPodDeviceInfoByHyperJob(hyperJob *vcbatchv1.HyperJob) {
	nds.podDeviceInfo = make(map[string]PodDeviceInfo)

	for _, rj := range hyperJob.Spec.ReplicatedJobs {
		for j := 0; j < int(rj.Replicas); j++ {
			jobName := GetRjName(hyperJob.Name, rj.Name, j)
			for _, task := range rj.Template.Tasks {
				if !isTaskRequestNpu(task) {
					continue
				}
				for i := 0; i < int(task.Replicas); i++ {
					podName := GetPodName(jobName, task.Name, i)
					nds.podDeviceInfo[podName] = PodDeviceInfo{
						DevInfo:  PodNpuInfo{},
						IsCached: false,
						// server_list按照job索引-pod索引排序
						SortIndex: fmt.Sprintf("%05d-%05d", j, i),
						// 用来聚合生成super_pod_list，相同的job属于同一个super_pod
						JobName: jobName,
					}
				}
			}
		}
	}
}

func (nds *NPUDeviceSyncer) UpdateNPUDevSyncer(owner interface{}) *ChangeEvent {
	switch job := owner.(type) {
	case *vcbatchv1.Job:
		return nds.updateNPUDevSyncerByJob(job)
	case *vcbatchv1.HyperJob:
		return nds.updateNPUDevSyncerByHyperJob(job)
	default:
		klog.V(3).Infof("Update NPU Dev syncer failed, only support Job/HyperJob")
		return nil
	}
}

func (nds *NPUDeviceSyncer) updateNPUDevSyncerByJob(newJob *vcbatchv1.Job) *ChangeEvent {
	oldJob, ok := nds.OwnerRef.Owner.(*vcbatchv1.Job)
	if !ok {
		klog.V(3).Infof("Update npu device syncer %s by job %s failed, old owner type is not Job",
			nds.Name, newJob.Name)
		return nil
	}

	// TODO 比较uuid
	if !isJobNeedUpdate(oldJob, newJob) {
		klog.V(5).Infof("Syncer %s owner spec is not changed", nds.Name)
		return nil
	}

	// Update owner reference to new job
	nds.OwnerRef.Owner = newJob

	return nds.updatePodDeviceInfoByJob(newJob)
}

func (nds *NPUDeviceSyncer) updatePodDeviceInfoByJob(newJob *vcbatchv1.Job) *ChangeEvent {
	expectedPods := make(map[string]struct{})
	addedPods := make(map[string]struct{})
	deletedPods := make(map[string]struct{})

	nds.Lock()
	defer nds.Unlock()
	for _, task := range newJob.Spec.Tasks {
		if !isTaskRequestNpu(task) {
			continue
		}
		for i := 0; i < int(task.Replicas); i++ {
			podName := GetPodName(newJob.Name, task.Name, i)
			expectedPods[podName] = struct{}{}

			// Add new pods
			if _, exists := nds.podDeviceInfo[podName]; !exists {
				nds.podDeviceInfo[podName] = PodDeviceInfo{
					DevInfo:   PodNpuInfo{},
					IsCached:  false,
					SortIndex: fmt.Sprintf("%05d", i),
				}
				addedPods[podName] = struct{}{}
				klog.V(4).Infof("Syncer %s added pod %+v to Pod device map",
					nds.Name, nds.podDeviceInfo[podName])
			}
		}
	}

	for podName := range nds.podDeviceInfo {
		if _, exists := expectedPods[podName]; !exists {
			delete(nds.podDeviceInfo, podName)
			deletedPods[podName] = struct{}{}
			klog.V(4).Infof("Syncer %s removed pod %s from Pod device map",
				nds.Name, podName)
		}
	}

	return nds.recordJobChangeEvent(addedPods, deletedPods)
}

func (nds *NPUDeviceSyncer) updateNPUDevSyncerByHyperJob(newHyperJob *vcbatchv1.HyperJob) *ChangeEvent {
	oldHyperJob, ok := nds.OwnerRef.Owner.(*vcbatchv1.HyperJob)
	if !ok {
		klog.V(3).Infof("Update npu device syncer %s by hyperJob %s failed, old owner type is not HyperJob",
			nds.Name, newHyperJob.Name)
		return nil
	}

	// TODO 比较uuid
	if !isHyperJobNeedUpdate(oldHyperJob, newHyperJob) {
		klog.V(5).Infof("Syncer %s owner spec is not changed", nds.Name)
		return nil
	}

	// Update owner reference to new hyperjob
	nds.OwnerRef.Owner = newHyperJob

	return nds.updatePodDeviceInfoByHyperJob(newHyperJob)
}

func (nds *NPUDeviceSyncer) updatePodDeviceInfoByHyperJob(newHyperJob *vcbatchv1.HyperJob) *ChangeEvent {
	expectedPods := make(map[string]struct{})
	addedPods := make(map[string]struct{})
	deletedPods := make(map[string]struct{})

	nds.Lock()
	defer nds.Unlock()
	// Build expected pod list from new hyperjob spec
	for _, rj := range newHyperJob.Spec.ReplicatedJobs {
		for j := 0; j < int(rj.Replicas); j++ {
			jobName := GetRjName(newHyperJob.Name, rj.Name, j)
			for _, task := range rj.Template.Tasks {
				if !isTaskRequestNpu(task) {
					continue
				}
				for i := 0; i < int(task.Replicas); i++ {
					podName := GetPodName(jobName, task.Name, i)
					expectedPods[podName] = struct{}{}

					// Add new pods
					if _, exists := nds.podDeviceInfo[podName]; !exists {
						nds.podDeviceInfo[podName] = PodDeviceInfo{
							DevInfo:   PodNpuInfo{},
							IsCached:  false,
							SortIndex: fmt.Sprintf("%05d-%05d", j, i),
							JobName:   jobName,
						}
						addedPods[podName] = struct{}{}
						klog.V(4).Infof("Syncer %s added pod %+v to Pod device map",
							nds.Name, nds.podDeviceInfo[podName])
					}
				}
			}
		}
	}

	// Remove obsolete pods
	for podName := range nds.podDeviceInfo {
		if _, exists := expectedPods[podName]; !exists {
			delete(nds.podDeviceInfo, podName)
			deletedPods[podName] = struct{}{}
			klog.V(4).Infof("Syncer %s removed pod %s from Pod device map",
				nds.Name, podName)
		}
	}

	return nds.recordJobChangeEvent(addedPods, deletedPods)
}

func (nds *NPUDeviceSyncer) recordJobChangeEvent(addedPods, deletedPods map[string]struct{}) *ChangeEvent {
	if len(addedPods) == 0 && len(deletedPods) == 0 {
		return nil
	}

	klog.V(3).Infof("Syncer %s spec changed, add %d pods, remove %d pods",
		nds.Name, len(addedPods), len(deletedPods))
	return &ChangeEvent{
		EventType: corev1.EventTypeNormal,
		Reason:    EventJobSpecUpdate,
		Message:   fmt.Sprintf("Job spec changed, add %d pods, remove %d pods", len(addedPods), len(deletedPods)),
	}
}

func (nds *NPUDeviceSyncer) SyncPodNpuDevs() *ChangeEvent {
	klog.V(4).Infof("Sync %s all pod device info begin", nds.Name)
	defer klog.V(4).Infof("Sync %s all pod device info end", nds.Name)

	nds.RLock()
	podNames := make([]string, 0, len(nds.podDeviceInfo))
	for podName := range nds.podDeviceInfo {
		podNames = append(podNames, podName)
	}
	nds.RUnlock()
	sort.Strings(podNames)
	podNum := len(podNames)

	var (
		podChangeList = make([]*PodEvent, 0, podNum)
		changeLock    sync.Mutex
	)

	concurrency := MaxWorkerCount
	if podNum < concurrency {
		concurrency = podNum
	}
	workqueue.ParallelizeUntil(context.TODO(), concurrency, podNum, func(i int) {
		podName := podNames[i]
		if event := nds.cacheDevInfo(podName); event != nil {
			changeLock.Lock()
			podChangeList = append(podChangeList, event)
			changeLock.Unlock()
		}
	})

	return recordSyncEvent(podChangeList)
}

func (nds *NPUDeviceSyncer) getPodByName(podName string) (*corev1.Pod, error) {
	return nds.podLister.Pods(nds.Namespace).Get(podName)
}

func (nds *NPUDeviceSyncer) cacheDevInfo(podName string) *PodEvent {
	nds.RLock()
	podDevInfo, exists := nds.podDeviceInfo[podName]
	nds.RUnlock()

	if !exists {
		klog.V(4).Infof("Pod %s not found in podDeviceInfo", podName)
		return nil
	}

	podEvent := PodEvent{PodName: podName}

	pod, err := nds.getPodByName(podName)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(4).Infof("Pod %s is not found", podName)
			return nds.clearDevInfo(podName)
		} else {
			klog.V(4).Infof("Get pod %s failed, err: %v", podName, err)
			return nil
		}
	}

	newDevInfo, err := getNpuDevInfo(pod)
	if err != nil {
		klog.V(4).Infof("Cache pod %v failed, %v", klog.KObj(pod), err)
		podEvent.EventType = EventDeviceParseErr
		nds.clearDevInfo(podName)
		return &podEvent
	}

	if newDevInfo == nil {
		klog.V(5).Infof("Pod %v npu device info is not ready", klog.KObj(pod))
		return nds.clearDevInfo(podName)
	}

	if reflect.DeepEqual(podDevInfo.DevInfo, *newDevInfo) {
		klog.V(5).Infof("Pod %v already cached", klog.KObj(pod))
		return nil
	}

	nds.Lock()
	defer nds.Unlock()
	if !podDevInfo.IsCached {
		podDevInfo.IsCached = true
		podEvent.EventType = EventDeviceCached
		klog.V(4).Infof("Pod %s/%s cached npu device: %+v", pod.Namespace, pod.Name, newDevInfo)
	} else {
		podEvent.EventType = EventDeviceModified
		klog.V(3).Infof("Pod %s/%s updated npu device, old: %+v, new: %+v",
			pod.Namespace, pod.Name, podDevInfo.DevInfo, newDevInfo)
	}
	podDevInfo.DevInfo = *newDevInfo
	nds.podDeviceInfo[pod.Name] = podDevInfo
	return &podEvent
}

func (nds *NPUDeviceSyncer) clearDevInfo(podName string) *PodEvent {
	podDevInfo := nds.podDeviceInfo[podName]

	if podDevInfo.IsCached {
		podDevInfo.DevInfo = PodNpuInfo{}
		podDevInfo.IsCached = false

		nds.Lock()
		defer nds.Unlock()
		nds.podDeviceInfo[podName] = podDevInfo
		klog.V(4).Infof("Clear pod %s npu device info", podName)
		return &PodEvent{PodName: podName, EventType: EventDeviceCleared}
	}

	return nil
}

func getNpuDevInfo(pod *corev1.Pod) (*PodNpuInfo, error) {
	npuDevInfo := &PodNpuInfo{}
	devInfoStr, exist := pod.Annotations[PodDeviceAnnotationKey]
	if !exist || devInfoStr == "" {
		klog.V(5).Infof("Cache pod %v failed, didn't contain annotation %s", klog.KObj(pod), PodDeviceAnnotationKey)
		return nil, nil
	}

	if err := npuDevInfo.UnmarshalJSON([]byte(devInfoStr)); err != nil {
		klog.V(3).Infof("Cache pod %v failed，parse annotation err: %v", klog.KObj(pod), err)
		return nil, fmt.Errorf("parse annotation err: %v", err)
	}

	if err := extractAndSortDevices(npuDevInfo, pod); err != nil {
		return nil, fmt.Errorf("parse annotation err: %v", err)
	}

	supplementTorInfo(npuDevInfo, pod)
	return npuDevInfo, nil
}

func extractAndSortDevices(npuDevInfo *PodNpuInfo, pod *corev1.Pod) error {
	if len(npuDevInfo.Devices) == 0 {
		return fmt.Errorf("no devices found in annotation")
	}

	sort.Slice(npuDevInfo.Devices, func(i, j int) bool {
		devIdI, err := strconv.Atoi(npuDevInfo.Devices[i].DeviceId)
		if err != nil {
			return npuDevInfo.Devices[i].DeviceId < npuDevInfo.Devices[j].DeviceId
		}
		devIdJ, err := strconv.Atoi(npuDevInfo.Devices[j].DeviceId)
		if err != nil {
			return npuDevInfo.Devices[i].DeviceId < npuDevInfo.Devices[j].DeviceId
		}
		return devIdI < devIdJ
	})
	return nil
}

// supplementTorInfo 补充设备的TOR信息
func supplementTorInfo(npuDevInfo *PodNpuInfo, pod *corev1.Pod) {
	if npuDevInfo.Devices[0].TorIp != "" {
		return
	}
	klog.V(4).Infof("Pod %v need supplement tor info", klog.KObj(pod))

	torInfo := &PodTorInfo{}
	torStr, exist := pod.Annotations[PodRankTableAnnotationKey]
	if !exist || torStr == "" {
		klog.V(4).Infof("Get pod %v anntation %s failed", klog.KObj(pod), PodRankTableAnnotationKey)
		return
	}

	if err := torInfo.UnmarshalJSON([]byte(torStr)); err != nil {
		klog.V(3).Infof("Get pod %v tor info failed，parse annotation err: %v", klog.KObj(pod), err)
		return
	}

	for i := range npuDevInfo.Devices {
		for _, torDev := range torInfo.Devices {
			if npuDevInfo.Devices[i].DeviceId == torDev.DeviceId {
				npuDevInfo.Devices[i].TorIp = torDev.TorIp
				npuDevInfo.Devices[i].TorPort = torDev.TorPort
				break
			}
		}
	}
}

func recordSyncEvent(podEventList []*PodEvent) *ChangeEvent {
	if len(podEventList) == 0 {
		return nil
	}

	var cached, cleared, modified, parseErr int

	for _, podEvent := range podEventList {
		switch podEvent.EventType {
		case EventDeviceCached:
			cached++
		case EventDeviceCleared:
			cleared++
		case EventDeviceModified:
			modified++
		case EventDeviceParseErr:
			parseErr++
		default:
			klog.V(3).Infof("Unknown pod event type %v", podEvent.EventType)
		}
	}

	var message strings.Builder
	message.WriteString("Job npu device sync result: ")

	parts := []struct {
		count int
		label string
	}{
		{cached, "cached"},
		{cleared, "cleared"},
		{modified, "modified"},
		{parseErr, "parseErr"},
	}

	first := true
	for _, part := range parts {
		if part.count > 0 {
			if !first {
				message.WriteString(", ")
			}
			message.WriteString(fmt.Sprintf("%s %d pods", part.label, part.count))
			first = false
		}
	}

	event := &ChangeEvent{
		EventType: corev1.EventTypeNormal,
		Reason:    EventJobSyncDevice,
		Message:   message.String(),
	}
	return event
}

func isTaskRequestNpu(taskSpec vcbatchv1.TaskSpec) bool {
	for _, c := range taskSpec.Template.Spec.Containers {
		quantity, exist := c.Resources.Limits[npuResourceName]
		if exist && quantity.Value() > 0 {
			return true
		}
	}
	return false
}

func isJobNeedUpdate(oldJob, newJob *vcbatchv1.Job) bool {
	if len(oldJob.Spec.Tasks) != len(newJob.Spec.Tasks) {
		return true
	}
	for i := range oldJob.Spec.Tasks {
		if oldJob.Spec.Tasks[i].Replicas != newJob.Spec.Tasks[i].Replicas {
			return true
		}
	}
	return false
}

func isHyperJobNeedUpdate(oldHj, newHj *vcbatchv1.HyperJob) bool {
	if len(oldHj.Spec.ReplicatedJobs) != len(newHj.Spec.ReplicatedJobs) {
		return true
	}
	for i := range oldHj.Spec.ReplicatedJobs {
		if oldHj.Spec.ReplicatedJobs[i].Replicas != newHj.Spec.ReplicatedJobs[i].Replicas {
			return true
		}
		// 添加对任务模板变化的检查
		if !reflect.DeepEqual(oldHj.Spec.ReplicatedJobs[i].Template, newHj.Spec.ReplicatedJobs[i].Template) {
			return true
		}
	}
	return false
}
