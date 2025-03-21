package ringcontroller

import (
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"testing"
	"time"
	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	"volcano.sh/apis/pkg/client/clientset/versioned/fake"
	"volcano.sh/apis/pkg/client/informers/externalversions"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func TestGetJobByEvent(t *testing.T) {
	fakeJob := &vcbatchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
	}

	fakeClient := fake.NewSimpleClientset(fakeJob)
	factory := externalversions.NewSharedInformerFactory(fakeClient, 0)
	rc := &RankTblController{
		vcSharedInformerFactory: factory,
		clients: RingClients{
			VcClient: fakeClient,
		},
	}
	rc.InitJobInformer()

	stopCh := make(chan struct{})
	go factory.Start(stopCh)
	defer close(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.jobInformer.HasSynced) {
		t.Fatal("timed out waiting for cache to sync")
	}

	event := SyncEvent{
		JobType:   "Job",
		Name:      "test-job",
		Namespace: "default",
	}

	job, err := rc.GetJobByEvent(event)
	assert.NoError(t, err)
	assert.NotNil(t, job)
	assert.Equal(t, fakeJob.Name, job.Name, "Job name does not match expected value")

	event = SyncEvent{
		JobType:   "Job",
		Name:      "invalid-job",
		Namespace: "default",
	}
	job, err = rc.GetJobByEvent(event)
	assert.Error(t, err)
	assert.Nil(t, job)
}

func TestHandleJobEvent(t *testing.T) {
	tests := []struct {
		name        string
		obj         interface{}
		expectedLen int
	}{
		{
			name: "valid job",
			obj: &vcbatchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Labels: map[string]string{
						cutil.Label1980Key: cutil.Label1980Value,
					},
				},
			},
			expectedLen: 1,
		},
		{
			name: "valid job belong to hyperjob",
			obj: &vcbatchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						vcbatchv1.HyperJobNameKey: "test-hyperjob",
					},
					Labels: map[string]string{
						cutil.Label1980Key: cutil.Label1980Value,
					},
				},
			},
			expectedLen: 1,
		},
		{
			name: "job is not care",
			obj: &vcbatchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
			},
			expectedLen: 0,
		},
		{
			name: "deleted job",
			obj: cache.DeletedFinalStateUnknown{
				Obj: &vcbatchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-job",
						Namespace: "default",
						Labels: map[string]string{
							cutil.Label1980Key: cutil.Label1980Value,
						},
					},
				},
			},
			expectedLen: 1,
		},
		{
			name:        "invalid object",
			obj:         "invalid",
			expectedLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RankTblController{
				queue: workqueue.NewTypedRateLimitingQueue[SyncEvent](
					workqueue.NewTypedMaxOfRateLimiter(
						workqueue.NewTypedItemExponentialFailureRateLimiter[SyncEvent](workQueueBaseDelay, workQueueMaxDelay),
						&workqueue.TypedBucketRateLimiter[SyncEvent]{Limiter: rate.NewLimiter(rate.Limit(workQueueRateLimit), workQueueBurst)},
					),
				),
			}
			rc.handleJobEvent(tt.obj)
			time.Sleep(50 * time.Millisecond)
			if tt.expectedLen != rc.queue.Len() {
				t.Errorf("Expected %d items in the queue but got %d", tt.expectedLen, rc.queue.Len())
			}
		})
	}
}

func TestCheckJobValid(t *testing.T) {
	tests := []struct {
		name     string
		job      *vcbatchv1.Job
		expected bool
	}{
		{
			name: "Job is being deleted",
			job: &vcbatchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job-deleting",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			expected: false,
		},
		{
			name: "Job is in completed state",
			job: &vcbatchv1.Job{
				Status: vcbatchv1.JobStatus{
					State: vcbatchv1.JobState{
						Phase: vcbatchv1.Completed,
					},
				},
			},
			expected: false,
		},
		{
			name: "Job is in terminated state",
			job: &vcbatchv1.Job{
				Status: vcbatchv1.JobStatus{
					State: vcbatchv1.JobState{
						Phase: vcbatchv1.Terminated,
					},
				},
			},
			expected: false,
		},
		{
			name: "Job is in valid state",
			job: &vcbatchv1.Job{
				Status: vcbatchv1.JobStatus{
					State: vcbatchv1.JobState{
						Phase: vcbatchv1.Running,
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RankTblController{}
			result := rc.CheckJobValid(tt.job)
			if result != tt.expected {
				t.Errorf("CheckJobValid(%s) = %v; want %v", tt.name, result, tt.expected)
			}
		})
	}
}
