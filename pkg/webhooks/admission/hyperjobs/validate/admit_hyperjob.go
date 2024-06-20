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

package validate

import (
	"fmt"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	whv1 "k8s.io/api/admissionregistration/v1"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/capabilities"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	hyperjobplugins "volcano.sh/volcano/pkg/controllers/hyperjob/plugins"
	"volcano.sh/volcano/pkg/webhooks/router"
	"volcano.sh/volcano/pkg/webhooks/schema"
	"volcano.sh/volcano/pkg/webhooks/util"
)

func init() {
	capabilities.Initialize(capabilities.Capabilities{
		AllowPrivileged: true,
		PrivilegedSources: capabilities.PrivilegedSources{
			HostNetworkSources: []string{},
			HostPIDSources:     []string{},
			HostIPCSources:     []string{},
		},
	})
	router.RegisterAdmission(service)
}

var service = &router.AdmissionService{
	Path: "/hyperjobs/validate",
	Func: AdmitHyperJobs,

	Config: config,

	ValidatingConfig: &whv1.ValidatingWebhookConfiguration{
		Webhooks: []whv1.ValidatingWebhook{{
			Name: "validatehyperjob.volcano.sh",
			Rules: []whv1.RuleWithOperations{
				{
					Operations: []whv1.OperationType{whv1.Create, whv1.Update},
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

// AdmitHyperJobs is to admit hyperjobs and return response.
func AdmitHyperJobs(ar admissionv1.AdmissionReview) *admissionv1.AdmissionResponse {
	klog.V(3).Infof("admitting hyperjobs -- %s", ar.Request.Operation)

	hyperJob, err := schema.DecodeHyperJob(ar.Request.Object, ar.Request.Resource)
	if err != nil {
		return util.ToAdmissionResponse(err)
	}
	var msg string
	reviewResponse := admissionv1.AdmissionResponse{}
	reviewResponse.Allowed = true

	switch ar.Request.Operation {
	case admissionv1.Create:
		msg = validateHyperJobCreate(hyperJob, &reviewResponse)
	case admissionv1.Update:
		oldHyperJob, err := schema.DecodeHyperJob(ar.Request.OldObject, ar.Request.Resource)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
		err = validateHyperJobUpdate(oldHyperJob, hyperJob)
		if err != nil {
			return util.ToAdmissionResponse(err)
		}
	default:
		err := fmt.Errorf("expect operation to be 'CREATE' or 'UPDATE'")
		return util.ToAdmissionResponse(err)
	}

	if !reviewResponse.Allowed {
		reviewResponse.Result = &metav1.Status{Message: strings.TrimSpace(msg)}
	}
	return &reviewResponse
}

func validateHyperJobCreate(hyperJob *vcbatch.HyperJob, reviewResponse *admissionv1.AdmissionResponse) string {
	var msg string

	//TODO Currently, the number of supported ReplicatedJobs is 1
	if len(hyperJob.Spec.ReplicatedJobs) > 1 {
		reviewResponse.Allowed = false
		return "The number of ReplicatedJobs can only be 1"
	}

	if len(hyperJob.Spec.ReplicatedJobs) == 0 {
		reviewResponse.Allowed = false
		return "No ReplicatedJobs specified in HyperJobSpec"
	}

	var replicasNum int32
	for _, rj := range hyperJob.Spec.ReplicatedJobs {
		if rj.Replicas < 0 {
			return fmt.Sprintf("replicas in ReplicatedJobs %s must be >= 0", rj.Name)
		}
		replicasNum += rj.Replicas
	}

	if hyperJob.Spec.MinAvailable > replicasNum {
		reviewResponse.Allowed = false
		return "'Spec.MinAvailable' should not be greater than total replicas in ReplicatedJobs"
	}

	if hyperJob.Spec.MinAvailable < 0 {
		reviewResponse.Allowed = false
		return "'Spec.MinAvailable' must be >= 0"
	}

	for len(hyperJob.Spec.Plugins) > 0 {
		for name := range hyperJob.Spec.Plugins {
			if _, found := hyperjobplugins.GetPluginBuilder(name); !found {
				msg += fmt.Sprintf("plugin %s is not found;", name)
			}
		}
	}

	if msg != "" {
		reviewResponse.Allowed = false
	}

	return msg
}

func validateHyperJobUpdate(old, new *vcbatch.HyperJob) error {
	if len(old.Spec.ReplicatedJobs) != len(new.Spec.ReplicatedJobs) {
		return fmt.Errorf("hyperjob updates may not add or remove ReplicatedJobs")
	}

	mungedSpec := new.Spec.DeepCopy()
	errs := apivalidation.ValidateImmutableField(mungedSpec.ReplicatedJobs, old.Spec.ReplicatedJobs, field.NewPath("spec").Child("replicatedJobs"))
	return errs.ToAggregate()
}
