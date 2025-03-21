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

package configmap1980

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"io"
)

const (
	Initializing = "initializing"
	Completed    = "completed"
)

//go:easyjson
type RankTable struct {
	Status       string         `json:"status"`
	Version      string         `json:"version"`
	DataVersion  int            `json:"data_version,omitempty"`
	ServerCount  string         `json:"server_count"`
	ServerList   []ServerBase   `json:"server_list"`
	SuperPodList []SuperPodBase `json:"super_pod_list,omitempty"`
}

type ServerBase struct {
	ServerId string       `json:"server_id"`
	Device   []DeviceBase `json:"device"`
}

type DeviceBase struct {
	DeviceId      string `json:"device_id"`
	SuperDeviceId string `json:"super_device_id,omitempty"`
	DeviceIp      string `json:"device_ip"`
	RankId        string `json:"rank_id"`
	TorIp         string `json:"tor_ip,omitempty"`
	TorPort       string `json:"tor_port,omitempty"`
}

type SuperPodBase struct {
	SuperPodId string           `json:"super_pod_id"`
	ServerList []SuperPodServer `json:"server_list"`
}

type SuperPodServer struct {
	ServerId string `json:"server_id"`
}

// TorList The definition of torList is used to generate the 910B tor information file ranktable_tor.json.
// It is no longer rigorous, and this file will no longer be generated in the future.
//
//go:easyjson
type TorList struct {
	Status      string      `json:"status"`
	Version     string      `json:"version"`
	ServerCount string      `json:"server_count"`
	ServerList  []TorServer `json:"server_list"`
}

type TorServer struct {
	PodName  string      `json:"pod_name"`
	ServerId string      `json:"server_id"`
	Device   []TorDevice `json:"device"`
}

type TorDevice struct {
	DeviceId string `json:"device_id"`
	DeviceIp string `json:"device_ip"`
	RankId   string `json:"rank_id"`
	TorIp    string `json:"tor_ip"`
	TorPort  string `json:"tor_port"`
}

func Compress(rankTable []byte) (string, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	if _, err := gz.Write(rankTable); err != nil {
		return "", err
	}
	if err := gz.Flush(); err != nil {
		return "", err
	}
	if err := gz.Close(); err != nil {
		return "", err
	}
	encoded := base64.StdEncoding.EncodeToString(b.Bytes())
	return encoded, nil
}

func Decompress(encodedStr string) ([]byte, error) {
	compressedData, err := base64.StdEncoding.DecodeString(encodedStr)
	if err != nil {
		return nil, err
	}

	gr, err := gzip.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	decompressed, err := io.ReadAll(gr)
	if err != nil {
		return nil, err
	}

	return decompressed, nil
}
