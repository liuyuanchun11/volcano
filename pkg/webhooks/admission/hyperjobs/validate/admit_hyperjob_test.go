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

package validate

import (
	"context"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	vcbatch "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	schedulingv1beta2 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	fakeclient "volcano.sh/apis/pkg/client/clientset/versioned/fake"
)

func TestValidateHyperJobCreate(t *testing.T) {
	testCases := []struct {
		name           string
		hyperJob       vcbatch.HyperJob
		expectErr      bool
		reviewResponse admissionv1.AdmissionResponse
		ret            string
	}{
		{
			name: "validate valid-hyperjob",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: true},
			ret:            "",
			expectErr:      false,
		},
		{
			name: "multi ReplicatedJobs",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "The number of ReplicatedJobs can only be 1",
			expectErr:      true,
		},
		{
			name: "no ReplicatedJobs",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					ReplicatedJobs: []vcbatch.ReplicatedJob{},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "No ReplicatedJobs specified in HyperJobSpec",
			expectErr:      true,
		},
		{
			name: "no replicatedJob name",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Template: vcbatch.JobSpec{},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "ReplicatedJobs[0] must have a name",
			expectErr:      true,
		},
		{
			name: "invalid replicatedJob.replicas",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: -1,
							Name:     "job-test",
							Template: vcbatch.JobSpec{},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "replicas in ReplicatedJobs[0] must be > 0",
			expectErr:      true,
		},
		{
			name: "minAvailable greater than replicas",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 8,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "'Spec.MinAvailable' should not be greater than total replicas in ReplicatedJobs",
			expectErr:      true,
		},
		{
			name: "invalid minAvailable",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: -1,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "'Spec.MinAvailable' must be >= 0",
			expectErr:      true,
		},
		{
			name: "invalid jobSpec.minAvailable",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: -1,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "template in ReplicatedJobs[0] err: job 'minAvailable' must be >= 0",
			expectErr:      true,
		},
		{
			name: "invalid podSpec",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
														Resources: v1.ResourceRequirements{
															Requests: map[v1.ResourceName]resource.Quantity{
																v1.ResourceCPU: resource.MustParse("-1"),
															},
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "template in ReplicatedJobs[0] err: spec.task[0].template.spec.containers[0].resources.requests[cpu]: Invalid value: \"-1\": must be greater than or equal to 0.",
			expectErr:      true,
		},
		{
			name: "invalid plugin",
			hyperJob: vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
					Plugins: map[string][]string{
						"invalid-test": {},
					},
				},
			},
			reviewResponse: admissionv1.AdmissionResponse{Allowed: false},
			ret:            "plugin invalid-test is not found",
			expectErr:      true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defaultqueue := schedulingv1beta2.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
				},
				Spec: schedulingv1beta2.QueueSpec{
					Weight: 1,
				},
				Status: schedulingv1beta2.QueueStatus{
					State: schedulingv1beta2.QueueStateOpen,
				},
			}

			// create fake volcano clientset
			config.VolcanoClient = fakeclient.NewSimpleClientset()

			//create default queue
			_, err := config.VolcanoClient.SchedulingV1beta1().Queues().Create(context.TODO(), &defaultqueue, metav1.CreateOptions{})
			if err != nil {
				t.Error("Queue Creation Failed")
			}

			ret := validateHyperJobCreate(&testCase.hyperJob, &testCase.reviewResponse)
			//fmt.Printf("test-case name:%s, ret:%v  testCase.reviewResponse:%v \n", testCase.Name, ret,testCase.reviewResponse)
			if testCase.expectErr == true && ret == "" {
				t.Errorf("Expect error msg :%s, but got nil.", testCase.ret)
			}
			if testCase.expectErr == true && testCase.reviewResponse.Allowed != false {
				t.Errorf("Expect Allowed as false but got true.")
			}
			if testCase.expectErr == true && !strings.Contains(ret, testCase.ret) {
				t.Errorf("Expect error msg :%s, but got diff error %v", testCase.ret, ret)
			}

			if testCase.expectErr == false && ret != "" {
				t.Errorf("Expect no error, but got error %v", ret)
			}
			if testCase.expectErr == false && testCase.reviewResponse.Allowed != true {
				t.Errorf("Expect Allowed as true but got false. %v", testCase.reviewResponse)
			}
		})
	}
}

func TestValidateHyperJobUpdate(t *testing.T) {
	oldHj := &vcbatch.HyperJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "hyperjob-test",
			Namespace: "default",
		},
		Spec: vcbatch.HyperJobSpec{
			MinAvailable: 2,
			ReplicatedJobs: []vcbatch.ReplicatedJob{
				{
					Replicas: 2,
					Name:     "job-test",
					Template: vcbatch.JobSpec{
						MinAvailable: 2,
						Queue:        "default",
						Tasks: []vcbatch.TaskSpec{
							{
								Name:     "worker",
								Replicas: 2,
								Template: v1.PodTemplateSpec{
									ObjectMeta: metav1.ObjectMeta{
										Labels: map[string]string{"name": "test"},
									},
									Spec: v1.PodSpec{
										Containers: []v1.Container{
											{
												Name:  "fake-name",
												Image: "busybox:1.24",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	testCases := []struct {
		name           string
		newHyperJob    *vcbatch.HyperJob
		expectErr      bool
		exceptErrorMsg string
	}{
		{
			name: "valid hyperjob",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 3,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 3,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      false,
			exceptErrorMsg: "",
		},
		{
			name: "invalid replicas in ReplicatedJob",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: -1,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "replicas in ReplicatedJobs[0] must be > 0",
		},
		{
			name: "invalid replicas in jobSpec",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: -1,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "spec in ReplicatedJobs[0] err: 'replicas' must be >= 0 in task: worker",
		},
		{
			name: "invalid minAvailable",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: -1,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "'Spec.MinAvailable' must be >= 0",
		},
		{
			name: "minAvailable greater than total replicas",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 8,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "'Spec.MinAvailable' should not be greater than total replicas in ReplicatedJobs",
		},
		{
			name: "add new ReplicatedJob",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 8,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
						{
							Replicas: 2,
							Name:     "job-test-new",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "hyperjob updates may not add or remove ReplicatedJobs",
		},
		{
			name: "modify replicasJob name",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test-new",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "hyperjob updates may not change fields other than `minAvailable`, `replicatedJobs[*].replicas under spec`",
		},
		{
			name: "modify task name",
			newHyperJob: &vcbatch.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "hyperjob-test",
					Namespace: "default",
				},
				Spec: vcbatch.HyperJobSpec{
					MinAvailable: 2,
					ReplicatedJobs: []vcbatch.ReplicatedJob{
						{
							Replicas: 2,
							Name:     "job-test",
							Template: vcbatch.JobSpec{
								MinAvailable: 2,
								Queue:        "default",
								Tasks: []vcbatch.TaskSpec{
									{
										Name:     "worker-new",
										Replicas: 2,
										Template: v1.PodTemplateSpec{
											ObjectMeta: metav1.ObjectMeta{
												Labels: map[string]string{"name": "test"},
											},
											Spec: v1.PodSpec{
												Containers: []v1.Container{
													{
														Name:  "fake-name",
														Image: "busybox:1.24",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectErr:      true,
			exceptErrorMsg: "job updates may not change fields other than `minAvailable`, `tasks[*].replicas under spec`",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ret := validateHyperJobUpdate(oldHj, testCase.newHyperJob)
			if testCase.expectErr == true && ret == nil {
				t.Errorf("Expect error msg :%s, but got nil.", testCase.exceptErrorMsg)
			}

			if testCase.expectErr == true && ret != nil && !strings.Contains(ret.Error(), testCase.exceptErrorMsg) {
				t.Errorf("Expect error msg :%s, but got diff error %v", testCase.exceptErrorMsg, ret)
			}

			if testCase.expectErr == false && ret != nil {
				t.Errorf("Expect no error, but got error %v", ret)
			}
		})
	}
}
