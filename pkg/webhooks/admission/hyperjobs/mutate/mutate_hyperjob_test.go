/*
Copyright 2019 The Volcano Authors.

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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

func TestCreatePatchExecution(t *testing.T) {
	var replicas int32
	replicas = 1

	testCases := []struct {
		name      string
		hyperJob  v1alpha1.HyperJob
		operation []patchOperation
	}{
		{
			name: "patch replicatedJob.replicas",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name: "vcjob-a",
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 1,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "patch queue",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "patch schedulerName",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								MaxRetry:     3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "patch maxRetry",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "patch replicatedJob.Template.minAvailable",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "patch replicatedJob.Template.minAvailable 2",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:         "task-test",
										Replicas:     2,
										MinAvailable: &replicas,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  1,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:         "task-test",
										Replicas:     2,
										MinAvailable: &replicas,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "patch minAvailable",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
						{
							Name:     "vcjob-b",
							Replicas: 4,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:    "add",
					Path:  "/spec/minAvailable",
					Value: 6,
				},
			},
		},
		{
			name: "exist minAvailable",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					MinAvailable: 6,
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 2,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
						{
							Name:     "vcjob-b",
							Replicas: 4,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: nil,
		},
		{
			name: "exist minAvailable, replicatedJob.replicas == 0",
			hyperJob: v1alpha1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: v1alpha1.HyperJobSpec{
					ReplicatedJobs: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 0,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
			},
			operation: []patchOperation{
				{
					Op:   "replace",
					Path: "/spec/replicatedJobs",
					Value: []v1alpha1.ReplicatedJob{
						{
							Name:     "vcjob-a",
							Replicas: 1,
							Template: v1alpha1.JobSpec{
								MinAvailable:  2,
								SchedulerName: "volcano",
								Queue:         "default",
								MaxRetry:      3,
								Tasks: []v1alpha1.TaskSpec{
									{
										Name:     "task-test",
										Replicas: 2,
									},
								},
							},
						},
					},
				},
				{
					Op:    "add",
					Path:  "/spec/minAvailable",
					Value: 1,
				},
			},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			patchBytes, _ := createPatch(&test.hyperJob)

			expectStr, err := json.Marshal(test.operation)
			if err != nil {
				t.Errorf("Testcase %s failed, err: marshal expect operation failed", test.name)
			}

			if string(patchBytes) != string(expectStr) {
				t.Errorf("Testcase %s failed, expect: %s, actual: %s", test.name, expectStr, patchBytes)
			}
		})
	}
}
