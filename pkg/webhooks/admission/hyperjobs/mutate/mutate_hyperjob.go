/*
Copyright 2018 The Volcano Authors.

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

package mutate

import (
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	whv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/klog/v2"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	commonutil "volcano.sh/volcano/pkg/util"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/schema"
	"volcano.sh/volcano/pkg/webhooks/util"
)

const (
	// DefaultQueue constant stores the name of the queue as "default"
	defaultQueue = "default"
	// DefaultMaxRetry is the default number of retries.
	defaultMaxRetry = 3
)

func init() {
	router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path: "/hyperjobs/mutate",
	Func: HyperJobs,

	Config: config,

	MutatingConfig: &whv1.MutatingWebhookConfiguration{
		Webhooks: []whv1.MutatingWebhook{{
			Name: "mutatehyperjob.volcano.sh",
			Rules: []whv1.RuleWithOperations{
				{
					Operations: []whv1.OperationType{whv1.Create},
					Rule: whv1.Rule{
						APIGroups:   []string{"batch.volcano.sh"},
						APIVersions: []string{"v1alpha1"},
						Resources:   []string{"hyperjobs"},
					},
				},
			},
		}},
	},
}

var config = &router.AdmissionServiceConfig{}

type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

// HyperJobs mutate hyperjobs.
func HyperJobs(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("mutating hyperjobs")

	hyperJob, err := schema.DecodeHyperJob(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return util.ToAdmissionResponse(err)
	}

	var patchBytes []byte
	switch ar.Request.Operation {
	case admissionv1.Create:
		patchBytes, _ = createPatch(hyperJob)
	default:
		err = fmt.Errorf("expect operation to be 'CREATE' ")
		return util.ToAdmissionResponse(err)
	}

	klog.V(3).Infof("AdmissionResponse: patch=%v", string(patchBytes))
	reviewResponse := admissionv1.AdmissionResponse{
		Allowed: true,
		Patch:   patchBytes,
	}
	if len(patchBytes) > 0 {
		pt := admissionv1.PatchTypeJSONPatch
		reviewResponse.PatchType = &pt
	}

	return &reviewResponse
}

func createPatch(hyperJob *vcbatch.HyperJob) ([]byte, error) {
	var patch []patchOperation

	pathReplicateJob := mutateReplicateJob(hyperJob.Spec.ReplicatedJobs, "/spec/replicatedJobs")
	if pathReplicateJob != nil {
		patch = append(patch, *pathReplicateJob)
	}

	klog.V(2).Infof("mutate hyperjob %+v", hyperJob)
	pathMinAvailable := patchMinAvailable(hyperJob)
	if pathMinAvailable != nil {
		patch = append(patch, *pathMinAvailable)
	}

	return json.Marshal(patch)
}

func patchMinAvailable(hyperJob *vcbatch.HyperJob) *patchOperation {
	if hyperJob.Spec.MinAvailable == 0 {
		var minAvailable int32
		for _, rj := range hyperJob.Spec.ReplicatedJobs {
			minAvailable += rj.Replicas
		}

		return &patchOperation{Op: "add", Path: "/spec/minAvailable", Value: minAvailable}
	}

	return nil
}

func mutateReplicateJob(replicatedJobs []vcbatch.ReplicatedJob, bashPath string) *patchOperation {
	patched := false

	for index := range replicatedJobs {
		if replicatedJobs[index].Replicas == 0 {
			replicatedJobs[index].Replicas = 1
			patched = true
		}

		if replicatedJobs[index].Template.Queue == "" {
			replicatedJobs[index].Template.Queue = defaultQueue
			patched = true
		}

		if replicatedJobs[index].Template.SchedulerName == "" {
			replicatedJobs[index].Template.SchedulerName = commonutil.GenerateSchedulerName(config.SchedulerNames)
			patched = true
		}

		if replicatedJobs[index].Template.MaxRetry == 0 {
			replicatedJobs[index].Template.MaxRetry = defaultMaxRetry
			patched = true
		}

		if replicatedJobs[index].Template.MinAvailable == 0 {
			var jobMinAvailable int32
			for _, task := range replicatedJobs[index].Template.Tasks {
				if task.MinAvailable != nil {
					jobMinAvailable += *task.MinAvailable
				} else {
					jobMinAvailable += task.Replicas
				}
			}
			replicatedJobs[index].Template.MinAvailable = jobMinAvailable
			patched = true
		}
	}

	if !patched {
		return nil
	}

	return &patchOperation{
		Op:    "replace",
		Path:  bashPath,
		Value: replicatedJobs,
	}
}
