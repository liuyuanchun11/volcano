package ringcontroller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func TestCalcNpuInfoStatus(t *testing.T) {
	testCases := []struct {
		name      string
		totalPod  int
		cachedPod int
		expected  string
	}{
		{"Initializing", 5, 3, configmap1980.Initializing},
		{"Completed", 5, 5, configmap1980.Completed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nds := &NPUDeviceSyncer{
				podDeviceInfo: make(map[string]PodDeviceInfo),
			}
			for i := 0; i < tc.totalPod; i++ {
				podName := fmt.Sprintf("pod-%d", i)
				isCached := i < tc.cachedPod
				nds.podDeviceInfo[podName] = PodDeviceInfo{
					IsCached:  isCached,
					SortIndex: "A",
					JobName:   "job1",
					DevInfo: PodNpuInfo{
						ServerId: "server-1",
						Devices: []NpuDevice{
							{DeviceId: "d1"},
							{DeviceId: "d2"},
						},
					},
				}
			}

			status := nds.calcNpuInfoStatus()
			assert.Equal(t, tc.expected, status)
		})
	}
}

func TestOrganizeAndSortServers(t *testing.T) {
	nds := &NPUDeviceSyncer{
		podDeviceInfo: map[string]PodDeviceInfo{
			"pod1": {
				SortIndex: "B",
				JobName:   "job1",
				DevInfo: PodNpuInfo{
					ServerId: "srv1",
					Devices: []NpuDevice{
						{DeviceId: "d3"},
						{DeviceId: "d1"},
					},
				},
			},
			"pod2": {
				SortIndex: "A",
				JobName:   "job2",
				DevInfo: PodNpuInfo{
					ServerId: "srv2",
					Devices: []NpuDevice{
						{DeviceId: "d2"},
					},
				},
			},
		},
	}

	servers := nds.organizeAndSortServer()
	assert.Len(t, servers, 2)

	assert.Equal(t, "pod2", servers[0].podName)
	assert.Equal(t, "pod1", servers[1].podName)

	devices := servers[1].devices
	assert.Equal(t, "d1", devices[0].DeviceId)
	assert.Equal(t, "d3", devices[1].DeviceId)
}

func TestGenerateRankTbl(t *testing.T) {
	nds := &NPUDeviceSyncer{
		Spec: RankTblSpec{
			RankTblVersion:   "v1",
			NeedSuperPodList: true,
			NeedTorList:      true,
		},
		podDeviceInfo: map[string]PodDeviceInfo{
			"pod1": {
				IsCached:  true,
				SortIndex: "A",
				JobName:   "job1",
				DevInfo: PodNpuInfo{
					ServerId: "srv1",
					Devices: []NpuDevice{
						{DeviceId: "d1"},
						{DeviceId: "d2"},
					},
				},
			},
		},
	}

	rankTbl, torList := nds.GenerateRankTbl()
	assert.NotNil(t, rankTbl)
	assert.NotNil(t, torList)
	assert.Equal(t, configmap1980.Completed, rankTbl.Status)
}

func TestAssignRankIds(t *testing.T) {
	oldServers := []configmap1980.ServerBase{
		{
			ServerId: "srv1",
			Device: []configmap1980.DeviceBase{
				{DeviceId: "d1", RankId: "0"},
				{DeviceId: "d2", RankId: "1"},
			},
		},
	}

	newServers := []configmap1980.ServerBase{
		{
			ServerId: "srv1",
			Device: []configmap1980.DeviceBase{
				{DeviceId: "d1"},
				{DeviceId: "d3"},
			},
		},
		{
			ServerId: "srv2",
			Device: []configmap1980.DeviceBase{
				{DeviceId: "d4"},
			},
		},
	}

	assignRankIds(oldServers, newServers)

	assert.Equal(t, "0", newServers[0].Device[0].RankId)
	assert.Equal(t, "2", newServers[0].Device[1].RankId)
	assert.Equal(t, "3", newServers[1].Device[0].RankId)
}

func TestCheckRankTblUpdate(t *testing.T) {
	oldTbl := &configmap1980.RankTable{
		Status: configmap1980.Completed,
		ServerList: []configmap1980.ServerBase{
			{ServerId: "srv1"},
		},
	}

	newTbl := &configmap1980.RankTable{
		Status: configmap1980.Completed,
		ServerList: []configmap1980.ServerBase{
			{ServerId: "srv1"},
			{ServerId: "srv2"},
		},
	}

	result, updated, reorder, _ := CheckRankTblUpdate(oldTbl, newTbl)
	assert.True(t, updated)
	assert.True(t, reorder)
	assert.Len(t, result.ServerList, 2)
}
