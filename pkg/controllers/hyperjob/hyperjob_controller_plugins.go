/*
Copyright 2024 The Volcano Authors.

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

package hyperjob

import (
	"fmt"

	"k8s.io/klog/v2"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	"volcano.sh/volcano/pkg/controllers/hyperjob/plugins"
	hjpluginsinterface "volcano.sh/volcano/pkg/controllers/hyperjob/plugins/interface"
)

func (hjr *HyperJobReconciler) pluginOnJobCreate(hyperJob *vcbatch.HyperJob, job *vcbatch.Job) error {
	client := hjpluginsinterface.PluginClientset{
		KubeClients: hjr.KubeClient,
		VcClients:   hjr.VcClient,
	}
	for name, args := range hyperJob.Spec.Plugins {
		pb, found := hyperjobplugins.GetPluginBuilder(name)
		if !found {
			err := fmt.Errorf("failed to get plugin %s", name)
			klog.Error(err)
			return err
		}
		klog.V(4).Infof("Starting to execute plugin at <pluginOnJobCreate>: %s on hyperJob: <%s/%s>", name, hyperJob.Namespace, hyperJob.Name)
		if err := pb(client, args).OnJobCreate(job, hyperJob); err != nil {
			klog.Errorf("Failed to process on job` create plugin %s, err %v.", name, err)
			return err
		}
	}
	return nil
}

func (hjr *HyperJobReconciler) pluginOnHyperJobAdd(hyperJob *vcbatch.HyperJob) error {
	client := hjpluginsinterface.PluginClientset{
		KubeClients: hjr.KubeClient,
		VcClients:   hjr.VcClient,
	}
	for name, args := range hyperJob.Spec.Plugins {
		pb, found := hyperjobplugins.GetPluginBuilder(name)
		if !found {
			err := fmt.Errorf("failed to get plugin %s", name)
			klog.Error(err)
			return err
		}
		klog.V(4).Infof("Starting to execute plugin at <pluginOnHyperJobAdd>: %s on hyperJob: <%s/%s>", name, hyperJob.Namespace, hyperJob.Name)
		if err := pb(client, args).OnHyperJobAdd(hyperJob); err != nil {
			klog.Errorf("Failed to process on hyperJob add plugin %s, err %v.", name, err)
			return err
		}
	}

	return nil
}
