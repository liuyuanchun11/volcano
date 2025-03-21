package ringcontroller

import (
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func TestNPUDevSyncerMap_AddNPUDevSyncer(t *testing.T) {
	nc := &NPUDevSyncerMap{}
	client := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(client, 0)
	podLister := factory.Core().V1().Pods().Lister()
	nc.Initialize(podLister)

	job := vcbatchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Spec: vcbatchv1.JobSpec{
			Plugins: map[string][]string{
				cutil.Name: {
					"--rank-table-compress=true",
					"--rank-table-version=1.2",
				},
			},
		},
	}

	hyperJob := vcbatchv1.HyperJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hyperjob",
			Namespace: "default",
		},
		Spec: vcbatchv1.HyperJobSpec{
			Plugins: map[string][]string{
				cutil.Name: {
					"--rank-table-compress=true",
					"--rank-table-version=1.2",
				},
			},
		},
	}

	tests := []struct {
		name       string
		owner      interface{}
		wantErr    error
		wantSyncer *NPUDeviceSyncer
	}{
		{
			name:    "add job syncer",
			owner:   &job,
			wantErr: nil,
			wantSyncer: &NPUDeviceSyncer{
				Name:      GetSyncerName(SyncTypeJob, job.Namespace, job.Name),
				Namespace: job.Namespace,
				OwnerRef: SyncerOwnerRef{
					Owner: &job,
				},
			},
		},
		{
			name:    "add hyperjob syncer",
			owner:   &hyperJob,
			wantErr: nil,
			wantSyncer: &NPUDeviceSyncer{
				Name:      GetSyncerName(SyncTypeHyperJob, hyperJob.Namespace, hyperJob.Name),
				Namespace: hyperJob.Namespace,
				OwnerRef: SyncerOwnerRef{
					Owner: &hyperJob,
				},
			},
		},
		{
			name:       "add invalid owner",
			owner:      "invalid-owner",
			wantErr:    fmt.Errorf("new syncer err: only support Job/HyperJob"),
			wantSyncer: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer, err := nc.AddNPUDevSyncer(tt.owner)
			if tt.wantErr != nil && err == nil {
				t.Errorf("Testcase <%s>, want error: <%v>, but got nil", tt.name, tt.wantErr)
			} else if tt.wantErr == nil && err != nil {
				t.Errorf("Testcase <%s>, want nil, but got error: <%v>", tt.name, err)
			} else if tt.wantErr != nil && err != nil {
				if tt.wantErr.Error() != err.Error() {
					t.Errorf("Testcase <%s>, want error: <%v>, but got error: <%v>", tt.name, tt.wantErr, err)
				}
			}

			if tt.wantSyncer != nil && syncer == nil {
				t.Errorf("Testcase <%s>, want syncer: <%v>, but got nil", tt.name, tt.wantSyncer)
			} else if tt.wantSyncer != nil && syncer != nil {
				if tt.wantSyncer.Name != syncer.Name || tt.wantSyncer.Namespace != syncer.Namespace ||
					tt.wantSyncer.OwnerRef.Owner != syncer.OwnerRef.Owner {
					t.Errorf("Testcase <%s>, want syncer: <%v>, but got syncer: <%v>", tt.name, tt.wantSyncer, syncer)
				}
			}
		})
	}
}

func TestNPUDevSyncerMap_GetNPUDevSyncer(t *testing.T) {
	nc := &NPUDevSyncerMap{}
	client := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(client, 0)
	podLister := factory.Core().V1().Pods().Lister()
	nc.Initialize(podLister)

	job := vcbatchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Spec: vcbatchv1.JobSpec{
			Plugins: map[string][]string{
				cutil.Name: {
					"--rank-table-compress=true",
					"--rank-table-version=1.2",
				},
			},
		},
	}

	syncer, err := nc.AddNPUDevSyncer(&job)
	if err != nil {
		t.Fatalf("Failed to add NPU device syncer: %v", err)
	}

	tests := []struct {
		name       string
		event      SyncEvent
		wantSyncer *NPUDeviceSyncer
	}{
		{
			name: "get existing syncer",
			event: SyncEvent{
				JobType:   SyncTypeJob,
				Namespace: job.Namespace,
				Name:      job.Name,
			},
			wantSyncer: syncer,
		},
		{
			name: "get non-existing syncer",
			event: SyncEvent{
				JobType:   SyncTypeJob,
				Namespace: "non-existing-namespace",
				Name:      "non-existing-name",
			},
			wantSyncer: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSyncer := nc.GetNPUDevSyncer(tt.event)
			if gotSyncer != tt.wantSyncer {
				t.Errorf("Testcase <%s>, want syncer: <%v>, but got syncer: <%v>", tt.name, tt.wantSyncer, gotSyncer)
			}
		})
	}
}

func TestNPUDevSyncerMap_DeleteNPUDevSyncer(t *testing.T) {
	nc := &NPUDevSyncerMap{}
	client := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(client, 0)
	podLister := factory.Core().V1().Pods().Lister()
	nc.Initialize(podLister)

	job := vcbatchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Spec: vcbatchv1.JobSpec{
			Plugins: map[string][]string{
				cutil.Name: {
					"--rank-table-compress=true",
					"--rank-table-version=1.2",
				},
			},
		},
	}

	_, err := nc.AddNPUDevSyncer(&job)
	if err != nil {
		t.Fatalf("Failed to add NPU device syncer: %v", err)
	}

	tests := []struct {
		name       string
		event      SyncEvent
		wantSyncer *NPUDeviceSyncer
	}{
		{
			name: "delete existing syncer",
			event: SyncEvent{
				JobType:   SyncTypeJob,
				Namespace: job.Namespace,
				Name:      job.Name,
			},
			wantSyncer: nil,
		},
		{
			name: "delete non-existing syncer",
			event: SyncEvent{
				JobType:   SyncTypeJob,
				Namespace: "non-existing-namespace",
				Name:      "non-existing-name",
			},
			wantSyncer: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nc.DeleteNPUDevSyncer(tt.event)
			gotSyncer := nc.GetNPUDevSyncer(tt.event)
			if gotSyncer != tt.wantSyncer {
				t.Errorf("Testcase <%s>, want syncer: <%v>, but got syncer: <%v>", tt.name, tt.wantSyncer, gotSyncer)
			}
		})
	}
}
