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
	"sync"

	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
)

type NPUDevSyncerMap struct {
	sync.RWMutex
	podLister listerv1.PodLister
	data      map[string]*NPUDeviceSyncer
}

func (nc *NPUDevSyncerMap) Initialize(podLister listerv1.PodLister) error {
	nc.podLister = podLister
	nc.data = make(map[string]*NPUDeviceSyncer)
	return nil
}

func (nc *NPUDevSyncerMap) AddNPUDevSyncer(owner interface{}) (*NPUDeviceSyncer, error) {
	syncer, err := NewNPUDevSyncer(nc.podLister, owner)
	if err != nil {
		return nil, fmt.Errorf("new syncer err: %v", err)
	}

	nc.Lock()
	defer nc.Unlock()
	nc.data[syncer.Name] = syncer
	return syncer, nil
}

func (nc *NPUDevSyncerMap) GetNPUDevSyncer(event SyncEvent) *NPUDeviceSyncer {
	syncerName := GetSyncerName(event.JobType, event.Namespace, event.Name)

	nc.RLock()
	defer nc.RUnlock()
	if syncer, exist := nc.data[syncerName]; exist {
		return syncer
	}
	return nil
}

func (nc *NPUDevSyncerMap) DeleteNPUDevSyncer(event SyncEvent) {
	syncerName := GetSyncerName(event.JobType, event.Namespace, event.Name)

	nc.Lock()
	defer nc.Unlock()
	_, exist := nc.data[syncerName]
	if !exist {
		klog.V(5).Infof("Delete npu device syncer %s failed, syncer not exist", syncerName)
		return
	}
	delete(nc.data, syncerName)
	klog.V(4).Infof("Delete npu device syncer %s success", syncerName)
}
