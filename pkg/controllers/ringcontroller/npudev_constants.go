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

// Pod annotation key
const (
	PodDeviceAnnotationKey    = "cce.kubectl.kubernetes.io/ascend-1980-configuration"
	PodRankTableAnnotationKey = "cce.kubectl.kubernetes.io/ascend-rank-table"
)

// PodNpuInfo json struct for annotation cce.kubectl.kubernetes.io/ascend-1980-configuration
// go:easyjson
type PodNpuInfo struct {
	PodName  string      `json:"pod_name"`
	ServerId string      `json:"server_id"`
	Devices  []NpuDevice `json:"devices"`
}

// go:easyjson
type NpuDevice struct {
	DeviceId      string `json:"device_id"`
	SuperDeviceId string `json:"super_device_id,omitempty"`
	DeviceIp      string `json:"device_ip"`
	TorIp         string `json:"tor_ip,omitempty"`
	TorPort       string `json:"tor_port,omitempty"`
}

// PodTorInfo json struct for annotation cce.kubectl.kubernetes.io/ascend-rank-table
// Compatible with older versions and no longer evolving
// go:easyjson
type PodTorInfo struct {
	PodName  string      `json:"pod_name"`
	ServerId string      `json:"server_id"`
	Devices  []TorDevice `json:"device"`
}

type TorDevice struct {
	DeviceId string `json:"device_id"`
	DeviceIp string `json:"device_ip"`
	RankId   string `json:"rank_id"`
	TorIp    string `json:"tor_ip"`
	TorPort  string `json:"tor_port"`
}
