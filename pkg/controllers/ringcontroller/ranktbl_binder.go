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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

const (
	maxRetries = 3
)

func (rc *RankTblController) GetOldRanktable(spec RankTblSpec) (*cutil.RankTable, error) {
	cm, err := rc.configmapLister.ConfigMaps(spec.Namespace).Get(spec.JobStartCmName)
	if err != nil {
		klog.V(4).Infof("Failed to get configmap %s/%s: %v", spec.Namespace, spec.JobStartCmName, err)
		return nil, fmt.Errorf("failed to get configmap %s/%s: %v", spec.Namespace, spec.JobStartCmName, err)
	}

	rankTbl, err := getJobStartHcclByCm(spec, cm)
	if err != nil {
		klog.V(4).Infof("Failed to get jobstart from configmap %v: %v", klog.KObj(cm), err)
		return nil, fmt.Errorf("failed to get jobstart_hccl.json: %v", err)
	}
	return rankTbl, nil
}

func getJobStartHcclByCm(spec RankTblSpec, cm *corev1.ConfigMap) (*cutil.RankTable, error) {
	rankTblStr, exist := cm.Data[cutil.MountJobStartHcclName]
	if !exist {
		return nil, fmt.Errorf("failed to find %s in configmap", cutil.MountJobStartHcclName)
	}
	klog.V(5).Infof("Retrieved jobstart_hccl data from configmap %v: %s", klog.KObj(cm), rankTblStr)

	if spec.RankTblCompress {
		decompressedStr, err := cutil.Decompress(rankTblStr)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress %s: %v", cutil.MountJobStartHcclName, err)
		}
		rankTblStr = string(decompressedStr)
	}

	var rankTbl cutil.RankTable
	err := rankTbl.UnmarshalJSON([]byte(rankTblStr))
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal %s: %v", cutil.MountJobStartHcclName, err)
	}
	klog.V(5).Infof("Unmarshalled jobstart_hccl from configmap %v: %+v", klog.KObj(cm), rankTbl)
	return &rankTbl, nil
}

func (rc *RankTblController) BindRankTbl2Pods(spec RankTblSpec, ranktable *cutil.RankTable,
	torList *cutil.TorList) ([]*ChangeEvent, error) {
	var changeList []*ChangeEvent

	event, err := rc.bindJobStartHccl(spec, ranktable)
	if event != nil {
		changeList = append(changeList, event)
	}
	if err != nil {
		return changeList, nil
	}

	if !spec.NeedTorList {
		return changeList, nil
	}
	event, err = rc.bindRankTableTor(spec, torList)
	if event != nil {
		changeList = append(changeList, event)
	}

	return changeList, err
}

func (rc *RankTblController) updateConfigMapWithRetry(cm *corev1.ConfigMap, updateFunc func(*corev1.ConfigMap) error) (*ChangeEvent, error) {
	event := ChangeEvent{}
	if err := updateFunc(cm); err != nil {
		event.EventType = corev1.EventTypeWarning
		event.Reason = EventUpdateCmFail
		event.Message = fmt.Sprintf("failed to update configmap %v: %v", klog.KObj(cm), err)
		return &event, fmt.Errorf("failed to update configmap %v: %v", klog.KObj(cm), err)
	}

	if err := rc.updateConfigmap(cm); err != nil {
		event.EventType = corev1.EventTypeWarning
		event.Reason = EventUpdateCmFail
		event.Message = fmt.Sprintf("failed to update configmap %v: %v", klog.KObj(cm), err)
		return &event, fmt.Errorf("failed to update configmap %v: %v", klog.KObj(cm), err)
	}

	event.EventType = corev1.EventTypeNormal
	event.Reason = EventUpdateCmSucc
	event.Message = fmt.Sprintf("configmap %v updated successfully", klog.KObj(cm))
	return &event, nil
}

func (rc *RankTblController) bindJobStartHccl(spec RankTblSpec, ranktable *cutil.RankTable) (*ChangeEvent, error) {
	cm, err := rc.configmapLister.ConfigMaps(spec.Namespace).Get(spec.JobStartCmName)
	if err != nil {
		return &ChangeEvent{
			EventType: corev1.EventTypeWarning,
			Reason:    EventUpdateCmFail,
			Message:   fmt.Sprintf("failed to get configmap %s/%s: %v", spec.Namespace, spec.JobStartCmName, err),
		}, fmt.Errorf("failed to get configmap %s/%s: %v", spec.Namespace, spec.JobStartCmName, err)
	}

	return rc.updateConfigMapWithRetry(cm, func(cm *corev1.ConfigMap) error {
		return updateJobStartHccl(cm, spec, ranktable)
	})
}

func updateJobStartHccl(cm *corev1.ConfigMap, spec RankTblSpec, ranktable *cutil.RankTable) error {
	dataVersion := 0
	curJobStartHccl, err := getJobStartHcclByCm(spec, cm)
	if err != nil {
		klog.V(3).Infof("Failed to get jobstart from configmap %v: %v", klog.KObj(cm), err)
	}
	dataVersion = curJobStartHccl.DataVersion

	for i := range ranktable.ServerList {
		for j := range ranktable.ServerList[i].Device {
			ranktable.ServerList[i].Device[j].TorIp = ""
			ranktable.ServerList[i].Device[j].TorPort = ""
		}
	}

	if spec.DataVersionEnable {
		ranktable.DataVersion = dataVersion + 1
	}

	jobStartBytes, err := ranktable.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal jobstart_hccl json: %v", err)
	}
	if spec.RankTblCompress {
		compressedStr, err := cutil.Compress(jobStartBytes)
		if err != nil {
			return fmt.Errorf("failed to compress jobstart_hccl json: %v", err)
		}
		cm.Data[cutil.MountJobStartHcclName] = compressedStr
		return nil
	}
	cm.Data[cutil.MountJobStartHcclName] = string(jobStartBytes)
	return nil
}

func (rc *RankTblController) bindRankTableTor(spec RankTblSpec, torList *cutil.TorList) (*ChangeEvent, error) {
	cm, err := rc.configmapLister.ConfigMaps(spec.Namespace).Get(spec.TorListCmName)
	if err != nil {
		return &ChangeEvent{
			EventType: corev1.EventTypeWarning,
			Reason:    EventUpdateCmFail,
			Message:   fmt.Sprintf("failed to get configmap %s/%s: %v", spec.Namespace, spec.TorListCmName, err),
		}, fmt.Errorf("failed to get configmap %s/%s: %v", spec.Namespace, spec.TorListCmName, err)
	}

	return rc.updateConfigMapWithRetry(cm, func(cm *corev1.ConfigMap) error {
		return updateRanktableTor(cm, torList)
	})
}

func updateRanktableTor(cm *corev1.ConfigMap, torList *cutil.TorList) error {
	ranktableTor, err := torList.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal ranktable_tor json: %v", err)
	}
	compressedStr, err := cutil.Compress(ranktableTor)
	if err != nil {
		return fmt.Errorf("failed to compress ranktable_tor json: %v", err)
	}

	cm.Data[cutil.MountRankTableTorName] = compressedStr
	return nil
}

func (rc *RankTblController) RecordChangeEvents(spec RankTblSpec, changeEventList []*ChangeEvent) {
	if len(changeEventList) == 0 {
		return
	}

	cm, err := rc.configmapLister.ConfigMaps(spec.Namespace).Get(spec.JobStartCmName)
	if err != nil {
		klog.V(4).Infof("Failed to record events to configmap %s/%s: %v", spec.Namespace, spec.JobStartCmName, err)
	}

	for _, event := range changeEventList {
		if event == nil || event.EventType == "" || event.Reason == "" {
			continue
		}
		rc.clients.Recorder.Eventf(cm, event.EventType, string(event.Reason), event.Message)
	}
}

func (rc *RankTblController) updateConfigmap(cm *corev1.ConfigMap) error {
	retryInterval := time.Duration(500) * time.Millisecond
	for retryCount := 0; retryCount <= maxRetries; retryCount++ {
		_, err := rc.clients.KubeClient.CoreV1().ConfigMaps(cm.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		if err == nil {
			return nil
		}
		klog.V(4).Infof("Failed to update configmap %v attempt %d: %v", klog.KObj(cm), retryCount+1, err)
		if retryCount >= maxRetries {
			return fmt.Errorf("failed to update configmap %v after %d attempts: %v", klog.KObj(cm), retryCount+1, err)
		}
		time.Sleep(retryInterval)
		retryInterval *= 2 // 指数退避
	}
	return nil
}
