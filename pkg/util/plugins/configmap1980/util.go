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
	"flag"
	"fmt"
	"strings"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

const (
	Name                 = "configmap1980"
	JobStartCmNamePrefix = "ranktable"
	TorListCmNamePrefix  = "torlist"

	RankTableVersion   = "rank-table-version"
	RankTableCompress  = "rank-table-compress"
	RankTableTorEnable = "rank-table-tor-enable"

	Label1980Key   = "ring-controller.cce"
	Label1980Value = "ascend-1980"
)

const (
	RankTblFormatV1 = "v1"
	RankTblFormatV2 = "v2"
	RankTblV10      = "1.0"
	RankTblV12      = "1.2"
	RankTblV13      = "1.3"
)

const (
	RankTableFilePath  = "RANK_TABLE_FILE"
	LogicSuperPodIDKey = "HCCL_LOGIC_SUPERPOD_ID"

	MountJobStartHcclName = "jobstart_hccl.json"
	MountRankTableTorName = "ranktable_tor.json"

	DefaultCmMountPath = "/user/config"
)

type CmArgs struct {
	RankTableVersion   string
	RankTableCompress  bool
	RankTableTorEnable bool
}

func ParsePluginArgs(args []string) (*CmArgs, error) {
	cmArgs := CmArgs{}

	flagSet := flag.NewFlagSet(Name, flag.ContinueOnError)
	flagSet.StringVar(&cmArgs.RankTableVersion, RankTableVersion, cmArgs.RankTableVersion,
		"set ranktable version")
	flagSet.BoolVar(&cmArgs.RankTableCompress, RankTableCompress, cmArgs.RankTableCompress,
		"set compress ranktable file")
	cmArgs.RankTableTorEnable = true
	flagSet.BoolVar(&cmArgs.RankTableTorEnable, RankTableTorEnable, cmArgs.RankTableTorEnable,
		"disable output of the ranktable tor file")
	if err := flagSet.Parse(args); err != nil {
		return nil, err
	}

	cmArgs.RankTableVersion = strings.ToLower(cmArgs.RankTableVersion)
	return &cmArgs, nil
}

func GetConfigmapName(jobName string) string {
	return fmt.Sprintf("%s-%s", JobStartCmNamePrefix, jobName)
}

func GetTorListCmName(jobName string) string {
	return fmt.Sprintf("%s-%s", TorListCmNamePrefix, jobName)
}

func GetPluginArgsByJob(job *vcbatchv1.Job) (*CmArgs, error) {
	args, exist := job.Spec.Plugins[Name]
	if !exist {
		return nil, fmt.Errorf("plugin %s not found", Name)
	}

	cmArgs, err := ParsePluginArgs(args)
	if err != nil {
		return nil, fmt.Errorf("parse plugin args err: %v", err)
	}

	if cmArgs.RankTableVersion == RankTblFormatV2 {
		cmArgs.RankTableVersion = RankTblV10
	}

	return cmArgs, nil
}

func GetPluginArgsByHyperJob(hyperJob *vcbatchv1.HyperJob) (*CmArgs, error) {
	args, exist := hyperJob.Spec.Plugins[Name]
	if !exist {
		return nil, fmt.Errorf("plugin %s not found", Name)
	}

	cmArgs, err := ParsePluginArgs(args)
	if err != nil {
		return nil, fmt.Errorf("parse plugin args err: %v", err)
	}

	if cmArgs.RankTableVersion == RankTblFormatV2 {
		cmArgs.RankTableVersion = RankTblV12
	}

	return cmArgs, nil
}
