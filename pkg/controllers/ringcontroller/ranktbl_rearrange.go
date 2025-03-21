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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/volcano/pkg/controllers/ringcontroller/plugins"
	"volcano.sh/volcano/pkg/controllers/ringcontroller/plugins/interface"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

const (
	rearrangePluginName = "rank-table-rearrange"

	JobObject        = "jobObject"
	ClusterHwVersion = "clusterHwVersion"

	dumyClusterHwVersion = "72_2"
)

func (rc *RankTblController) RanktableRearrange(nds *NPUDeviceSyncer, oldRankTbl *cutil.RankTable,
	rankTbl *cutil.RankTable) (*cutil.RankTable, *ChangeEvent) {
	pluginArgs := getRearrangePluginArgs(nds)
	if pluginArgs == nil {
		klog.V(3).Infof("Syncer %s rearrange capability is not enabled", nds.Name)
		return rankTbl, nil
	}

	pb, found := plugins.GetPluginBuilder(rearrangePluginName)
	if !found {
		klog.V(3).Infof("Syncer %s ranktable rearrange get plugin %s failed", nds.Name, rearrangePluginName)
		return rankTbl, nil
	}

	args, err := constructRearrangeArgs(nds)
	if err != nil {
		klog.V(3).Infof("Syncer %s get rearrange construct arguments failed, err: %v", nds.Name, err)
		return rankTbl, nil
	}

	client := pluginsinterface.PLuginClientset{
		KubeClients: rc.clients.KubeClient,
	}
	start := time.Now()
	rankTableArranged, err := pb(client, pluginArgs).OnRanktableCompleted(rankTbl, args)
	if err != nil {
		klog.V(3).Infof("Syncer %s ranktable rearrange failed, err: %v", nds.Name, err)
		return rankTbl, &ChangeEvent{
			EventType: corev1.EventTypeWarning,
			Reason:    EventRankTblRrgFail,
			Message:   fmt.Sprintf("Ranktable rearrange failed, err: %v", err),
		}
	}
	elapsed := time.Since(start)
	klog.V(3).Infof("Syncer %s ranktable rearrange success, cost %v ms, ranktable: %+v",
		nds.Name, elapsed.Milliseconds(), rankTableArranged)
	return rankTableArranged, &ChangeEvent{
		EventType: corev1.EventTypeNormal,
		Reason:    EventRankTblRrgSucc,
		Message:   fmt.Sprintf("Ranktable is successfully rearranged, cost %v ms", elapsed.Milliseconds()),
	}
}

func (rc *RankTblController) RanktableRearrangeRelease(nds *NPUDeviceSyncer) {
	pluginArgs := getRearrangePluginArgs(nds)
	if pluginArgs == nil {
		return
	}

	pb, found := plugins.GetPluginBuilder(rearrangePluginName)
	if !found {
		klog.V(3).Infof("Syncer %s ranktable rearrange releasse get plugin %s failed", nds.Name, rearrangePluginName)

		return
	}

	args, err := constructRearrangeArgs(nds)
	if err != nil {
		klog.V(3).Infof("Syncer %s get rearrange construct arguments failed, err: %v", nds.Name, err)
		return
	}

	client := pluginsinterface.PLuginClientset{
		KubeClients: rc.clients.KubeClient,
	}

	err = pb(client, pluginArgs).OnRanktableDelete(args)
	if err != nil {
		klog.V(3).Infof("Syncer %s ranktable rearrange release failed, err: %v", nds.Name, err)
	}
	klog.V(3).Infof("Syncer %s ranktable rearrange release", nds.Name)
}

func getRearrangePluginArgs(nds *NPUDeviceSyncer) []string {
	var pluginArgs []string
	var exist bool

	switch nds.OwnerRef.Owner.(type) {
	case *vcbatchv1.Job:
		job := nds.OwnerRef.Owner.(*vcbatchv1.Job)
		pluginArgs, exist = job.Spec.Plugins[rearrangePluginName]
		if !exist {
			return nil
		}
	case *vcbatchv1.HyperJob:
		hyperJob := nds.OwnerRef.Owner.(*vcbatchv1.HyperJob)
		pluginArgs, exist = hyperJob.Spec.Plugins[rearrangePluginName]
		if !exist {
			return nil
		}
	default:
		klog.V(3).Infof("Syncer %s has invalid owner type", nds.Name)
		return nil
	}

	return pluginArgs
}

func constructRearrangeArgs(nds *NPUDeviceSyncer) (map[string]interface{}, error) {
	args := make(map[string]interface{})

	args[JobObject] = nds.OwnerRef.Owner

	switch nds.OwnerRef.Owner.(type) {
	case *vcbatchv1.Job:
		// do nothing
	case *vcbatchv1.HyperJob:
		args[ClusterHwVersion] = dumyClusterHwVersion
	default:
		return nil, fmt.Errorf("invalid owner type %v", nds.OwnerRef.Owner)
	}
	return args, nil
}
