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
)

const (
	SyncTypeJob      = "job"
	SyncTypeHyperJob = "hyperJob"
)

type SyncEvent struct {
	JobType   string
	Namespace string
	Name      string
}

func (e SyncEvent) GetKey() string {
	return fmt.Sprintf("%s/%s/%s", e.JobType, e.Namespace, e.Name)
}

type ChangeEventType string

const (
	EventJobSpecUpdate  ChangeEventType = "JobSpecUpdate"
	EventJobSyncDevice  ChangeEventType = "JobSyncDevice"
	EventRankTblUpdate  ChangeEventType = "RankTblUpdate"
	EventRankTblRrgSucc ChangeEventType = "RankTblRearrangeSuccess"
	EventRankTblRrgFail ChangeEventType = "RankTblRearrangeFail"
	EventUpdateCmSucc   ChangeEventType = "UpdateConfigmapSuccess"
	EventUpdateCmFail   ChangeEventType = "UpdateConfigmapFail"
)

type ChangeEvent struct {
	EventType string
	Reason    ChangeEventType
	Message   string
}

type RankTblSpec struct {
	Namespace string

	// rankTable参数
	RankTblVersion    string
	NeedSuperPodList  bool
	RankTblCompress   bool
	DataVersionEnable bool
	JobStartCmName    string

	// torList参数
	NeedTorList   bool
	TorListCmName string
}

func GetSyncerName(jobType, namespace, name string) string {
	return fmt.Sprintf("%s/%s/%s", jobType, namespace, name)
}

func GetPodName(jobName, taskName string, taskIndex int) string {
	return fmt.Sprintf("%s-%s-%d", jobName, taskName, taskIndex)
}

func GetRjName(hyperJobName, replicatedJobName string, rjIndex int) string {
	return fmt.Sprintf("%s-%s-%d", hyperJobName, replicatedJobName, rjIndex)
}
