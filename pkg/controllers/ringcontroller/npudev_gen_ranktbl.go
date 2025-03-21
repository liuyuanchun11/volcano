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
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

// 节点npu数量预估最大为16
const avgDevicesPerServer = 16

type npuServer struct {
	podName   string
	sortIndex string
	jobName   string
	serverId  string
	devices   []cutil.DeviceBase
}

func (nds *NPUDeviceSyncer) GenerateRankTbl() (*cutil.RankTable, *cutil.TorList) {
	newRankTbl := &cutil.RankTable{
		Status:  cutil.Initializing,
		Version: nds.Spec.RankTblVersion,
	}

	status := nds.calcNpuInfoStatus()
	if status == cutil.Initializing {
		return newRankTbl, nil
	}

	newRankTbl.Status = cutil.Completed
	servers := nds.organizeAndSortServer()
	newRankTbl.ServerList = generateServerList(servers)
	newRankTbl.ServerCount = strconv.Itoa(len(newRankTbl.ServerList))
	if nds.Spec.NeedSuperPodList {
		newRankTbl.SuperPodList = generateSuperPodList(servers)
	}
	klog.V(5).Infof("Generate %s ranktable: %+v", nds.Name, newRankTbl)

	torList := &cutil.TorList{
		Status:  cutil.Initializing,
		Version: "1.0",
	}
	if nds.Spec.NeedTorList {
		torList = generateTorList(servers)
		klog.V(5).Infof("Generate %s torlist: %+v", nds.Name, torList)
	}

	return newRankTbl, torList
}

func (nds *NPUDeviceSyncer) calcNpuInfoStatus() string {
	var totalPod int
	var cachedPod int

	nds.RLock()
	defer nds.RUnlock()
	totalPod = len(nds.podDeviceInfo)
	for _, podDevInfo := range nds.podDeviceInfo {
		if podDevInfo.IsCached {
			cachedPod++
		}
	}

	var status string
	klog.V(4).Infof("Syncer %s total pod %d, cached pod %d", nds.Name, totalPod, cachedPod)
	if totalPod > cachedPod {
		status = cutil.Initializing
	} else if totalPod == cachedPod {
		status = cutil.Completed
	} else {
		klog.V(3).Infof("Syncer %s total pod %d is less than cached pod %d", nds.Name, totalPod, cachedPod)
		status = cutil.Initializing
	}

	return status
}

func (nds *NPUDeviceSyncer) organizeAndSortServer() []npuServer {
	serverMap := make(map[string]npuServer)

	nds.RLock()
	defer nds.RUnlock()
	for podName, deviceInfo := range nds.podDeviceInfo {
		serverId := deviceInfo.DevInfo.ServerId
		newServer, exist := serverMap[serverId]
		if !exist {
			newServer = npuServer{
				podName:   podName,
				sortIndex: deviceInfo.SortIndex,
				jobName:   deviceInfo.JobName,
				serverId:  serverId,
				devices:   make([]cutil.DeviceBase, 0, avgDevicesPerServer),
			}
		}

		// 批量添加设备并保持有序
		tmpDevices := make([]cutil.DeviceBase, 0, len(deviceInfo.DevInfo.Devices))
		for _, dev := range deviceInfo.DevInfo.Devices {
			tmpDevices = append(tmpDevices, cutil.DeviceBase{
				DeviceId:      dev.DeviceId,
				SuperDeviceId: dev.SuperDeviceId,
				DeviceIp:      dev.DeviceIp,
				TorIp:         dev.TorIp,
				TorPort:       dev.TorPort,
			})
		}

		newServer.devices = append(newServer.devices, tmpDevices...)
		serverMap[serverId] = newServer
	}

	servers := make([]npuServer, 0, len(serverMap))
	for _, server := range serverMap {
		sort.Slice(server.devices, func(i, j int) bool {
			devIdI, err := strconv.Atoi(server.devices[i].DeviceId)
			if err != nil {
				return server.devices[i].DeviceId < server.devices[j].DeviceId
			}
			devIdJ, err := strconv.Atoi(server.devices[j].DeviceId)
			if err != nil {
				return server.devices[i].DeviceId < server.devices[j].DeviceId
			}
			return devIdI < devIdJ
		})
		servers = append(servers, server)
	}

	sort.Slice(servers, func(i, j int) bool {
		return servers[i].sortIndex < servers[j].sortIndex
	})
	return servers
}

func generateServerList(servers []npuServer) []cutil.ServerBase {
	serverList := make([]cutil.ServerBase, len(servers))
	for i := range servers {
		serverList[i] = cutil.ServerBase{
			ServerId: servers[i].serverId,
			Device:   servers[i].devices,
		}
	}

	return serverList
}

func generateSuperPodList(servers []npuServer) []cutil.SuperPodBase {
	// 使用slice代替map来保持顺序
	var superPodList []cutil.SuperPodBase
	var currentSuperPod *cutil.SuperPodBase

	for _, server := range servers {
		if currentSuperPod == nil || currentSuperPod.SuperPodId != server.jobName {
			// 发现新的SuperPod时创建新条目
			if currentSuperPod != nil {
				superPodList = append(superPodList, *currentSuperPod)
			}
			currentSuperPod = &cutil.SuperPodBase{
				SuperPodId: server.jobName,
				ServerList: make([]cutil.SuperPodServer, 0, 1), // 预分配最小容量
			}
		}
		// 直接追加已排序的服务器ID（利用npuServerMap已排序的特性）
		currentSuperPod.ServerList = append(currentSuperPod.ServerList, cutil.SuperPodServer{
			ServerId: server.serverId,
		})
	}

	// 添加最后一个superPod
	if currentSuperPod != nil {
		superPodList = append(superPodList, *currentSuperPod)
	}

	return superPodList
}

func generateTorList(servers []npuServer) *cutil.TorList {
	torList := &cutil.TorList{
		Status:  cutil.Initializing,
		Version: "1.0",
	}

	for _, server := range servers {
		torServer := cutil.TorServer{
			ServerId: server.serverId,
			PodName:  server.podName,
			Device:   make([]cutil.TorDevice, 0, len(server.devices)),
		}
		for _, device := range server.devices {
			torDevice := cutil.TorDevice{
				DeviceId: device.DeviceId,
				DeviceIp: device.DeviceIp,
				TorIp:    device.TorIp,
				TorPort:  device.TorPort,
			}
			torServer.Device = append(torServer.Device, torDevice)
		}
		torList.ServerList = append(torList.ServerList, torServer)
	}

	torList.ServerCount = strconv.Itoa(len(torList.ServerList))
	return torList
}
func assignRankIds(oldServerList []cutil.ServerBase, newServerList []cutil.ServerBase) {
	// 收集新服务器列表中的所有 ServerId
	existServerIds := make(map[string]struct{})
	for _, server := range newServerList {
		existServerIds[server.ServerId] = struct{}{}
	}

	// 创建映射来存储旧服务器中存在的设备的 RankId（仅当旧服务器存在于新列表时）
	oldRankMap := make(map[string]map[string]string) // serverId -> deviceId -> rankId
	usedRanks := make(map[int]struct{})

	// 填充旧 RankId 映射和已使用的 RankId 集合
	for _, server := range oldServerList {
		serverId := server.ServerId
		// 仅处理存在于新服务器列表中的旧服务器
		if _, exist := existServerIds[serverId]; !exist {
			continue
		}
		oldRankMap[serverId] = make(map[string]string)
		for _, device := range server.Device {
			rankId, err := strconv.Atoi(device.RankId)
			if err != nil {
				klog.Warningf("Invalid RankId %s for device %s in server %s", device.RankId, device.DeviceId, serverId)
				continue
			}
			oldRankMap[serverId][device.DeviceId] = device.RankId
			usedRanks[rankId] = struct{}{}
		}
	}

	// 遍历新服务器列表，分配 RankId
	for i := range newServerList {
		server := &newServerList[i]
		serverId := server.ServerId
		for j := range server.Device {
			device := &server.Device[j]
			deviceId := device.DeviceId

			// 检查是否存在旧的 RankId（仅当旧服务器存在于新列表时）
			if devices, ok := oldRankMap[serverId]; ok {
				if rankIdStr, ok := devices[deviceId]; ok {
					device.RankId = rankIdStr
					continue
				}
			}

			// 分配新的连续 RankId（从 0 开始找最小的未使用值）
			newRank := 0
			for {
				if _, exists := usedRanks[newRank]; !exists {
					break
				}
				newRank++
			}
			device.RankId = strconv.Itoa(newRank)
			usedRanks[newRank] = struct{}{}
		}
	}
}

// CheckRankTblUpdate 检查并更新 RankTable 的状态和内容。
// 该函数根据新旧 RankTable 的状态和服务器列表的变化，决定是否需要更新 RankTable，并生成相应的事件。
// 处理逻辑如下：
// | 新 RankTable 状态  | 旧 RankTable 状态  | serverList变化         | 操作                                                                              |
// |-------------------|-------------------|-----------------------|-----------------------------------------------------------------------------------|
// | Initializing      | Completed         | NA                    | 更新状态为 Initializing，保留旧的 ServerList，需要更新                                 |
// | Initializing      | Initializing      | NA                    | 不需要更新                                                                          |
// | Completed         | Completed         | ServerList 未变化      | 不需要更新                                                                          |
// | Completed         | Completed         | ServerList 发生变化    | 未变更的 Server 继承旧的 RankId，新增 Server 分配新的 RankId，需要更新。暂时不支持局部重编排 |
// | Completed         | Initializing      | NA                    | 未变更的 Server 继承旧的 RankId，新增 Server 分配新的 RankId，需要更新，整体重编排         |
// | 其他状态           | -                 | 非法状态                | 记录日志，不更新，返回 false                                                          |
func CheckRankTblUpdate(oldRankTbl *cutil.RankTable,
	newRankTbl *cutil.RankTable) (*cutil.RankTable, bool, bool, *ChangeEvent) {
	event := ChangeEvent{
		EventType: corev1.EventTypeNormal,
		Reason:    EventRankTblUpdate,
	}

	switch newRankTbl.Status {
	case cutil.Initializing:
		if oldRankTbl.Status == cutil.Completed {
			// 只更新状态，保留原有的serverList
			newRankTbl = oldRankTbl
			newRankTbl.Status = cutil.Initializing
			event.Message = fmt.Sprintf("ranktable status update to %s", cutil.Initializing)
			return newRankTbl, true, false, &event
		} else {
			return newRankTbl, false, false, nil
		}
	case cutil.Completed:
		if oldRankTbl.Status == cutil.Completed {
			if !isServerListEqual(newRankTbl.ServerList, oldRankTbl.ServerList) {
				// 未变更server继承原有rank id， 新增server重新分配
				assignRankIds(oldRankTbl.ServerList, newRankTbl.ServerList)
				event.Message = "ranktable server list modified"
				// serverList 有变更，需要更新并且重排
				return newRankTbl, true, true, &event
			}
		} else {
			// 未变更server继承原有rank id， 新增server重新分配
			assignRankIds(oldRankTbl.ServerList, newRankTbl.ServerList)
			event.Message = fmt.Sprintf("ranktable status update to %s", cutil.Completed)
			return newRankTbl, true, true, &event
		}
	default:
		klog.V(3).Infof("Invalid ranktable status %s", newRankTbl.Status)
	}
	return newRankTbl, false, false, nil
}

func isServerListEqual(newList []cutil.ServerBase, oldList []cutil.ServerBase) bool {
	klog.V(5).Infof("Compare serverList, old serverList: %+v", oldList)
	klog.V(5).Infof("Compare serverList, new serverList: %+v", newList)
	if len(newList) != len(oldList) {
		return false
	}

	// 缓存设备数量，避免重复计算
	for i := range newList {
		newServer := newList[i]
		oldServer := oldList[i]

		if newServer.ServerId != oldServer.ServerId {
			return false
		}

		newDevLen := len(newServer.Device)
		oldDevLen := len(oldServer.Device)
		if newDevLen != oldDevLen {
			return false
		}

		// 设备已预排序，可以直接顺序比较
		for j := 0; j < newDevLen; j++ {
			newDev := newServer.Device[j]
			oldDev := oldServer.Device[j]

			// 比较除RankId外的所有字段
			if newDev.DeviceId != oldDev.DeviceId ||
				newDev.SuperDeviceId != oldDev.SuperDeviceId ||
				newDev.DeviceIp != oldDev.DeviceIp {
				return false
			}
		}
	}
	return true
}
