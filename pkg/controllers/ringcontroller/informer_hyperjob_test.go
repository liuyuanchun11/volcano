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

func TestGetHyperJobByEvent(t *testing.T) {
	fakeHyperJob := &vcbatchv1.HyperJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-hyper-job",
			Namespace: "default",
		},
	}

	fakeClient := fake.NewSimpleClientset(fakeHyperJob)
	factory := externalversions.NewSharedInformerFactory(fakeClient, 0)
	rc := &RankTblController{
		vcSharedInformerFactory: factory,
		clients: RingClients{
			VcClient: fakeClient,
		},
	}
	rc.InitHyperJobInformer()

	stopCh := make(chan struct{})
	go factory.Start(stopCh)
	defer close(stopCh)
	if !cache.WaitForCacheSync(stopCh, rc.hyperJobInformer.HasSynced) {
		t.Fatal("timed out waiting for cache to sync")
	}

	event := SyncEvent{
		JobType:   "HyperJob",
		Name:      "test-hyper-job",
		Namespace: "default",
	}

	hyperJob, err := rc.GetHyperJobByEvent(event)
	assert.NoError(t, err)
	assert.NotNil(t, hyperJob)
	assert.Equal(t, fakeHyperJob.Name, hyperJob.Name, "HyperJob name does not match expected value")

	event = SyncEvent{
		JobType:   "HyperJob",
		Name:      "invalid-hyperJob",
		Namespace: "default",
	}
	hyperJob, err = rc.GetHyperJobByEvent(event)
	assert.Error(t, err)
	assert.Nil(t, hyperJob)
}

func TestHandleHyperJobEvent(t *testing.T) {
	tests := []struct {
		name        string
		obj         interface{}
		expectedLen int
	}{
		{
			name: "valid hyperJob",
			obj: &vcbatchv1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-hyper-job",
					Namespace: "default",
					Labels: map[string]string{
						cutil.Label1980Key: cutil.Label1980Value,
					},
				},
			},
			expectedLen: 1,
		},
		{
			name: "hyperJob is not care",
			obj: &vcbatchv1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-hyper-job",
					Namespace: "default",
				},
			},
			expectedLen: 0,
		},
		{
			name: "deleted hyper-job",
			obj: cache.DeletedFinalStateUnknown{
				Obj: &vcbatchv1.HyperJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-hyper-job",
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
			rc.handleHyperJobEvent(tt.obj)
			time.Sleep(50 * time.Millisecond)
			if tt.expectedLen != rc.queue.Len() {
				t.Errorf("Expected %d items in the queue but got %d", tt.expectedLen, rc.queue.Len())
			}
		})
	}
}

func TestCheckHyperJobValid(t *testing.T) {
	tests := []struct {
		name     string
		hyperJob *vcbatchv1.HyperJob
		expected bool
	}{
		{
			name: "HyperJob is being deleted",
			hyperJob: &vcbatchv1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			expected: false,
		},
		{
			name: "HyperJob is in Failed condition",
			hyperJob: &vcbatchv1.HyperJob{
				Status: vcbatchv1.HyperJobStatus{
					Conditions: []metav1.Condition{
						{
							Type:   string(vcbatchv1.HyperJobFailed),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "HyperJob is in Completed condition",
			hyperJob: &vcbatchv1.HyperJob{
				Status: vcbatchv1.HyperJobStatus{
					Conditions: []metav1.Condition{
						{
							Type:   string(vcbatchv1.HyperJobCompleted),
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "HyperJob is valid",
			hyperJob: &vcbatchv1.HyperJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Status: vcbatchv1.HyperJobStatus{
					Conditions: []metav1.Condition{},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RankTblController{}
			result := rc.CheckHyperJobValid(tt.hyperJob)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
