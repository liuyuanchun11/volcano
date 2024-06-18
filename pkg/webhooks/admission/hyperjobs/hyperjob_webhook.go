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
	"context"
	"errors"
	"fmt"

	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"

	hyperjobplugins "volcano.sh/volcano/pkg/controllers/hyperjob/plugins"
)

type HyperJobWebhook struct {
	client  client.Client
	decoder *admission.Decoder
}

func NewHyperJobWebhook(client client.Client) (*HyperJobWebhook, error) {
	return &HyperJobWebhook{client: client}, nil
}

func (hj *HyperJobWebhook) InjectDecoder(decoder *admission.Decoder) error {
	hj.decoder = decoder
	return nil
}

func (hj *HyperJobWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&vcbatch.HyperJob{}).
		WithDefaulter(hj).
		WithValidator(hj).
		Complete()
}

func (hj *HyperJobWebhook) Default(ctx context.Context, obj runtime.Object) error {
	hyperJob, ok := obj.(*vcbatch.HyperJob)
	if !ok {
		return nil
	}

	if hyperJob.Spec.MinAvailable == 0 {
		var minAvailable int32
		for _, rj := range hyperJob.Spec.ReplicatedJobs {
			minAvailable += rj.Replicas
		}
		hyperJob.Spec.MinAvailable = minAvailable
	}

	if hyperJob.Spec.StartupPolicy == nil {
		hyperJob.Spec.StartupPolicy = &vcbatch.StartupPolicy{
			StartupPolicyOrder: vcbatch.AnyOrder,
		}
	}

	if hyperJob.Spec.SuccessPolicy == nil {
		hyperJob.Spec.SuccessPolicy = &vcbatch.SuccessPolicy{
			Operator: vcbatch.OperatorAll,
		}
	}

	return nil
}

func (hj *HyperJobWebhook) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	hyperJob, ok := obj.(*vcbatch.HyperJob)
	if !ok {
		return nil, fmt.Errorf("expected a HyperJob but got a %T", obj)
	}
	var allErrs []error

	//TODO Currently, the number of supported ReplicatedJobs is 1
	if len(hyperJob.Spec.ReplicatedJobs) > 1 {
		allErrs = append(allErrs, fmt.Errorf("the number of ReplicatedJobs can only be 1"))
	}

	rjName := make(map[string]struct{})
	for _, rj := range hyperJob.Spec.ReplicatedJobs {
		if _, exist := rjName[rj.Name]; exist {
			allErrs = append(allErrs, fmt.Errorf("the name of ReplicatedJobs %s is duplicated", rj.Name))
		}
		rjName[rj.Name] = struct{}{}

		if rj.Replicas < 0 {
			allErrs = append(allErrs, fmt.Errorf("replicas in ReplicatedJobs %s must be >= 0", rj.Name))
		}
	}

	if hyperJob.Spec.MinAvailable < 0 {
		allErrs = append(allErrs, fmt.Errorf("MinAvailable must be >= 0"))
	}

	for len(hyperJob.Spec.Plugins) > 0 {
		for name := range hyperJob.Spec.Plugins {
			if _, found := hyperjobplugins.GetPluginBuilder(name); !found {
				allErrs = append(allErrs, fmt.Errorf("plugin %s is not found", name))
			}
		}
	}

	return nil, errors.Join(allErrs...)
}

func (hj *HyperJobWebhook) ValidateUpdate(ctx context.Context, old, newObj runtime.Object) (admission.Warnings, error) {
	hyperJob, ok := newObj.(*vcbatch.HyperJob)
	if !ok {
		return nil, fmt.Errorf("expected a JobSet but got a %T", newObj)
	}
	oldHj, ok := old.(*vcbatch.HyperJob)
	if !ok {
		return nil, fmt.Errorf("expected a JobSet from old object but got a %T", old)
	}
	mungedSpec := hyperJob.Spec.DeepCopy()

	errs := apivalidation.ValidateImmutableField(mungedSpec.ReplicatedJobs, oldHj.Spec.ReplicatedJobs, field.NewPath("spec").Child("replicatedJobs"))
	return nil, errs.ToAggregate()
}

func (hj *HyperJobWebhook) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}
