package ringcontroller

import (
	"golang.org/x/time/rate"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func TestInitPodInformer(t *testing.T) {
	rc := &RankTblController{
		clients: RingClients{
			KubeClient: fake.NewSimpleClientset(),
		},
	}
	rc.kubeSharedInformerFactory = informers.NewSharedInformerFactory(rc.clients.KubeClient, 0)

	err := rc.InitPodInformer()
	if err != nil {
		t.Errorf("InitPodInformer failed: %v", err)
	}

	if rc.podInformer == nil {
		t.Error("podInformer not initialized")
	}

	if rc.podLister == nil {
		t.Error("podLister not initialized")
	}
}

func TestNewPodHandler(t *testing.T) {
	rc := &RankTblController{}
	handler := rc.newPodHandler()

	if handler == nil {
		t.Error("Expected non-nil handler")
	}
}

func TestHandlePodEvent(t *testing.T) {
	value := true
	tests := []struct {
		name        string
		obj         interface{}
		expectedLen int
	}{
		{
			name: "valid pod belong job",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1alpha1",
							Kind:       "Job",
							Controller: &value,
						},
					},
					Annotations: map[string]string{
						vcbatchv1.JobNameKey: "test-job",
					},
					Labels: map[string]string{
						cutil.Label1980Key: cutil.Label1980Value,
					},
				},
			},
			expectedLen: 1,
		},
		{
			name: "valid pod belong hyperjob",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1alpha1",
							Kind:       "Job",
							Controller: &value,
						},
					},
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
			name: "valid pod dont' care",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1alpha1",
							Kind:       "Job",
							Controller: &value,
						},
					},
					Annotations: map[string]string{
						vcbatchv1.HyperJobNameKey: "test-hyperjob",
					},
				},
			},
			expectedLen: 0,
		},
		{
			name: "deleted pod",
			obj: cache.DeletedFinalStateUnknown{
				Obj: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-pod",
						Namespace: "default",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "batch.volcano.sh/v1alpha1",
								Kind:       "Job",
								Controller: &value,
							},
						},
						Annotations: map[string]string{
							vcbatchv1.JobNameKey: "test-job",
						},
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
			rc.handlePodEvent(tt.obj)
			time.Sleep(50 * time.Millisecond)
			if tt.expectedLen != rc.queue.Len() {
				t.Errorf("Expected %d items in the queue but got %d", tt.expectedLen, rc.queue.Len())
			}
		})
	}
}

func TestCheckPodCare(t *testing.T) {
	value := true
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name: "valid pod",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1alpha1",
							Kind:       "Job",
							Controller: &value,
						},
					},
					Labels: map[string]string{
						cutil.Label1980Key: cutil.Label1980Value,
					},
				},
			},
			expected: true,
		},
		{
			name: "missing label",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1alpha1",
							Kind:       "Job",
							Controller: &value,
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "wrong controller",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "v1",
							Kind:       "ReplicaSet",
						},
					},
					Labels: map[string]string{
						cutil.Label1980Key: cutil.Label1980Value,
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checkPodCare(tt.pod)
			if result != tt.expected {
				t.Errorf("Expected %v but got %v", tt.expected, result)
			}
		})
	}
}

func TestIsControlledBy(t *testing.T) {
	value := true
	tests := []struct {
		name     string
		obj      metav1.Object
		gvk      schema.GroupVersionKind
		expected bool
	}{
		{
			name: "valid controller",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1alpha1",
							Kind:       "Job",
							Controller: &value,
						},
					},
				},
			},
			gvk: schema.GroupVersionKind{
				Group:   "batch.volcano.sh",
				Version: "v1alpha1",
				Kind:    "Job",
			},
			expected: true,
		},
		{
			name: "wrong version",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch.volcano.sh/v1beta1",
							Kind:       "Job",
							Controller: &value,
						},
					},
				},
			},
			gvk: schema.GroupVersionKind{
				Group:   "batch.volcano.sh",
				Version: "v1alpha1",
				Kind:    "Job",
			},
			expected: false,
		},
		{
			name: "no controller",
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
			},
			gvk: schema.GroupVersionKind{
				Group:   "batch.volcano.sh",
				Version: "v1alpha1",
				Kind:    "Job",
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsControlledBy(tt.obj, tt.gvk)
			if result != tt.expected {
				t.Errorf("Expected %v but got %v", tt.expected, result)
			}
		})
	}
}

func TestGetOwnerByPod(t *testing.T) {
	tests := []struct {
		name        string
		pod         *corev1.Pod
		expectedErr bool
	}{
		{
			name: "hyperjob owner",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						vcbatchv1.HyperJobNameKey: "test-hyperjob",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "job owner",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						vcbatchv1.JobNameKey: "test-job",
					},
				},
			},
			expectedErr: false,
		},
		{
			name: "no owner",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{},
			},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := getOwnerByPod(tt.pod)
			if (err != nil) != tt.expectedErr {
				t.Errorf("Expected error %v but got %v", tt.expectedErr, err)
			}
		})
	}
}
