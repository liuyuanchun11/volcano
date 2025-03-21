package ringcontroller

import (
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	cutil "volcano.sh/volcano/pkg/util/plugins/configmap1980"
)

func TestNewNPUDevSyncer(t *testing.T) {
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
			Tasks: []vcbatchv1.TaskSpec{
				{
					Name:     "task1",
					Replicas: 8,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											npuResourceName: resource.MustParse("2"),
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
			ReplicatedJobs: []vcbatchv1.ReplicatedJob{
				{
					Name:     "rj1",
					Replicas: 2,
					Template: vcbatchv1.JobSpec{
						Tasks: []vcbatchv1.TaskSpec{
							{
								Name:     "task1",
								Replicas: 8,
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name: "container",
												Resources: corev1.ResourceRequirements{
													Limits: map[corev1.ResourceName]resource.Quantity{
														npuResourceName: resource.MustParse("1"),
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

func TestUpdateNPUDevSyncer(t *testing.T) {
	nc := &NPUDevSyncerMap{}
	client := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(client, 0)
	podLister := factory.Core().V1().Pods().Lister()
	nc.Initialize(podLister)

	job := &vcbatchv1.Job{
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
			Tasks: []vcbatchv1.TaskSpec{
				{
					Name:     "task1",
					Replicas: 8,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											npuResourceName: resource.MustParse("2"),
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

	jobReplicasInc := job.DeepCopy()
	jobReplicasInc.Spec.Tasks[0].Replicas = 16

	jobReplicasDec := job.DeepCopy()
	jobReplicasDec.Spec.Tasks[0].Replicas = 4

	hyperJob := &vcbatchv1.HyperJob{
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
			ReplicatedJobs: []vcbatchv1.ReplicatedJob{
				{
					Name:     "rj1",
					Replicas: 2,
					Template: vcbatchv1.JobSpec{
						Tasks: []vcbatchv1.TaskSpec{
							{
								Name:     "task1",
								Replicas: 8,
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name: "container",
												Resources: corev1.ResourceRequirements{
													Limits: map[corev1.ResourceName]resource.Quantity{
														npuResourceName: resource.MustParse("1"),
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
	}

	hyperJobInc := hyperJob.DeepCopy()
	hyperJobInc.Spec.ReplicatedJobs[0].Replicas = 4

	hyperJobDec := hyperJob.DeepCopy()
	hyperJobDec.Spec.ReplicatedJobs[0].Replicas = 1

	tests := []struct {
		name      string
		oldOwner  interface{}
		owner     interface{}
		wantEvent *ChangeEvent
	}{
		{
			name:      "job is no need to update",
			oldOwner:  job,
			owner:     job,
			wantEvent: nil,
		},
		{
			name:     "job replicas increase",
			oldOwner: job,
			owner:    jobReplicasInc,
			wantEvent: &ChangeEvent{
				EventType: corev1.EventTypeNormal,
				Reason:    EventJobSpecUpdate,
				Message:   "Job spec changed, add 8 pods, remove 0 pods",
			},
		},
		{
			name:     "job replicas decrease",
			oldOwner: job,
			owner:    jobReplicasDec,
			wantEvent: &ChangeEvent{
				EventType: corev1.EventTypeNormal,
				Reason:    EventJobSpecUpdate,
				Message:   "Job spec changed, add 0 pods, remove 4 pods",
			},
		},
		{
			name:      "hyperjob is no need to update",
			oldOwner:  hyperJob,
			owner:     hyperJob,
			wantEvent: nil,
		},
		{
			name:     "hyperjob replicas increase",
			oldOwner: hyperJob,
			owner:    hyperJobInc,
			wantEvent: &ChangeEvent{
				EventType: corev1.EventTypeNormal,
				Reason:    EventJobSpecUpdate,
				Message:   "Job spec changed, add 16 pods, remove 0 pods",
			},
		},
		{
			name:     "hyperjob replicas decrease",
			oldOwner: hyperJob,
			owner:    hyperJobDec,
			wantEvent: &ChangeEvent{
				EventType: corev1.EventTypeNormal,
				Reason:    EventJobSpecUpdate,
				Message:   "Job spec changed, add 0 pods, remove 8 pods",
			},
		},
		{
			name:      "hyperjob update by job",
			oldOwner:  hyperJob,
			owner:     job,
			wantEvent: nil,
		},
		{
			name:      "job update by hyperjob",
			oldOwner:  job,
			owner:     hyperJob,
			wantEvent: nil,
		},
		{
			name:      "job update by invalid owner",
			oldOwner:  job,
			owner:     "invalid owner",
			wantEvent: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			syncer, err := nc.AddNPUDevSyncer(tt.oldOwner)
			if err != nil {
				t.Errorf("Testcase <%s>, add syncer failed, error: <%v>", tt.name, err)
			}

			event := syncer.UpdateNPUDevSyncer(tt.owner)
			if tt.wantEvent == nil && event != nil {
				t.Errorf("Testcase <%s>, want nil, but got event: <%v>", tt.name, event)
			} else if tt.wantEvent != nil && event == nil {
				t.Errorf("Testcase <%s>, want event: <%v>, but got nil", tt.name, tt.wantEvent)
			} else if tt.wantEvent != nil && event != nil {
				if tt.wantEvent.EventType != event.EventType || tt.wantEvent.Reason != event.Reason ||
					tt.wantEvent.Message != event.Message {
					t.Errorf("Testcase <%s>, want event: <%v>, but got event: <%v>", tt.name, tt.wantEvent, event)
				}
			}
		})
	}
}

var mockPodMap map[string]*corev1.Pod

func mockDevInfo(podName string) string {
	rand.Seed(time.Now().UnixNano())
	devInfo := &PodNpuInfo{
		PodName:  podName,
		ServerId: net.IPv4(byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))).String(),
		Devices:  make([]NpuDevice, 16),
	}

	for i := 0; i < 16; i++ {
		deviceIp := net.IPv4(byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))).String()
		torIp := net.IPv4(byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))).String()
		port := rand.Intn(65535)

		device := NpuDevice{
			DeviceId: strconv.Itoa(i),
			DeviceIp: deviceIp,
			TorIp:    torIp,
			TorPort:  strconv.Itoa(port),
		}
		devInfo.Devices[i] = device
	}

	devInfoStr, _ := devInfo.MarshalJSON()
	return string(devInfoStr)
}

func mockPodByJob(job *vcbatchv1.Job) {
	mockPodMap = make(map[string]*corev1.Pod)
	namePrefix := job.Name + "-" + job.Spec.Tasks[0].Name

	replicas := int(job.Spec.Tasks[0].Replicas)
	for i := 0; i < replicas; i++ {
		podName := fmt.Sprintf("%s-%d", namePrefix, i)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: "default",
				Annotations: map[string]string{
					PodDeviceAnnotationKey: mockDevInfo(podName),
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "container",
						Resources: corev1.ResourceRequirements{
							Limits: map[corev1.ResourceName]resource.Quantity{
								npuResourceName: resource.MustParse("1"),
							},
						},
					},
				},
			},
		}

		mockPodMap[podName] = pod
	}
}

func mockPod(pod *corev1.Pod) {
	mockPodMap[pod.Name] = pod
}

func deleteMockPod(podName string) {
	delete(mockPodMap, podName)
}

func mockGetPodByName(_ *NPUDeviceSyncer, podName string) (*corev1.Pod, error) {
	pod, exist := mockPodMap[podName]
	if !exist {
		return nil, fmt.Errorf("pod %s not found", podName)
	}
	return pod, nil
}

func TestSyncPodNpuDevs(t *testing.T) {
	var ns *NPUDeviceSyncer
	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(ns), "getPodByName", mockGetPodByName)
	defer patch.Reset()

	job := &vcbatchv1.Job{
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
			Tasks: []vcbatchv1.TaskSpec{
				{
					Name:     "worker",
					Replicas: 128,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											npuResourceName: resource.MustParse("1"),
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

	var podLister listerv1.PodLister
	syncer, err := NewNPUDevSyncer(podLister, job)
	if err != nil {
		t.Errorf("Testcase <%s>, add syncer failed, error: <%v>", "test-job", err)
		t.Failed()
	}

	mockPodByJob(job)
	event := syncer.SyncPodNpuDevs()
	if event == nil {
		t.Errorf("Testcase <%s>, want event, but got nil", "test-job")
		t.Failed()
	}
	mockPodMap = make(map[string]*corev1.Pod)
}

func TestGetNpuDevInfo(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pod",
			Namespace:   "default",
			Annotations: make(map[string]string),
		},
	}
	tests := []struct {
		name       string
		annotation map[string]string
		devInfo    *PodNpuInfo
		wantErr    error
	}{
		{
			name:       "no annotation",
			annotation: nil,
			devInfo:    nil,
			wantErr:    nil,
		},
		{
			name: "pod with valid annotation",
			annotation: map[string]string{
				PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
			},
			devInfo: &PodNpuInfo{
				PodName:  "test-pod",
				ServerId: "101.96.44.5",
				Devices: []NpuDevice{
					{
						DeviceId: "0",
						DeviceIp: "127.126.229.111",
						TorIp:    "247.254.203.180",
						TorPort:  "27010",
					},
					{
						DeviceId: "1",
						DeviceIp: "159.229.27.93",
						TorIp:    "143.64.180.240",
						TorPort:  "21749",
					},
					{
						DeviceId: "2",
						DeviceIp: "178.4.196.138",
						TorIp:    "204.254.19.64",
						TorPort:  "17786",
					},
					{
						DeviceId: "3",
						DeviceIp: "30.54.154.194",
						TorIp:    "168.140.225.254",
						TorPort:  "3215",
					},
					{
						DeviceId: "4",
						DeviceIp: "190.174.86.247",
						TorIp:    "146.111.14.186",
						TorPort:  "64140",
					},
					{
						DeviceId: "5",
						DeviceIp: "246.251.52.59",
						TorIp:    "88.121.23.97",
						TorPort:  "39079",
					},
					{
						DeviceId: "6",
						DeviceIp: "158.158.67.96",
						TorIp:    "62.96.59.180",
						TorPort:  "56262",
					},
					{
						DeviceId: "7",
						DeviceIp: "20.27.228.103",
						TorIp:    "143.133.145.191",
						TorPort:  "47173",
					},
					{
						DeviceId: "8",
						DeviceIp: "83.218.52.8",
						TorIp:    "54.147.170.75",
						TorPort:  "3560",
					},
					{
						DeviceId: "9",
						DeviceIp: "188.87.127.234",
						TorIp:    "234.105.55.184",
						TorPort:  "41237",
					},
					{
						DeviceId: "10",
						DeviceIp: "54.6.152.104",
						TorIp:    "22.119.161.173",
						TorPort:  "51954",
					},
					{
						DeviceId: "11",
						DeviceIp: "205.18.66.191",
						TorIp:    "36.9.191.254",
						TorPort:  "7621",
					},
					{
						DeviceId: "12",
						DeviceIp: "233.149.120.143",
						TorIp:    "106.91.50.146",
						TorPort:  "64589",
					},
					{
						DeviceId: "13",
						DeviceIp: "153.52.51.185",
						TorIp:    "154.90.213.239",
						TorPort:  "23984",
					},
					{
						DeviceId: "14",
						DeviceIp: "18.205.248.88",
						TorIp:    "191.241.16.21",
						TorPort:  "4881",
					},
					{
						DeviceId: "15",
						DeviceIp: "221.30.122.53",
						TorIp:    "234.75.179.217",
						TorPort:  "53818",
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "annotation parse failed",
			annotation: map[string]string{
				PodDeviceAnnotationKey: "invalid-annotation",
			},
			devInfo: nil,
			wantErr: fmt.Errorf("parse annotation err: parse error: syntax error near offset 0 of 'invalid-an...'"),
		},
		{
			name: "annotation no devices",
			annotation: map[string]string{
				PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[]}",
			},
			devInfo: nil,
			wantErr: fmt.Errorf("parse annotation err: no devices found in annotation"),
		},
		{
			name: "pod with valid annotation with tor info",
			annotation: map[string]string{
				PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\"}]}",
			},
			devInfo: &PodNpuInfo{
				PodName:  "test-pod",
				ServerId: "101.96.44.5",
				Devices: []NpuDevice{
					{
						DeviceId: "0",
						DeviceIp: "127.126.229.111",
					},
					{
						DeviceId: "1",
						DeviceIp: "159.229.27.93",
					},
					{
						DeviceId: "2",
						DeviceIp: "178.4.196.138",
					},
					{
						DeviceId: "3",
						DeviceIp: "30.54.154.194",
					},
					{
						DeviceId: "4",
						DeviceIp: "190.174.86.247",
					},
					{
						DeviceId: "5",
						DeviceIp: "246.251.52.59",
					},
					{
						DeviceId: "6",
						DeviceIp: "158.158.67.96",
					},
					{
						DeviceId: "7",
						DeviceIp: "20.27.228.103",
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "pod with valid tor annotation",
			annotation: map[string]string{
				PodDeviceAnnotationKey:    "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\"}]}",
				PodRankTableAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"device\":[{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"}]}",
			},
			devInfo: &PodNpuInfo{
				PodName:  "test-pod",
				ServerId: "101.96.44.5",
				Devices: []NpuDevice{
					{
						DeviceId: "0",
						DeviceIp: "127.126.229.111",
						TorIp:    "247.254.203.180",
						TorPort:  "27010",
					},
					{
						DeviceId: "1",
						DeviceIp: "159.229.27.93",
						TorIp:    "143.64.180.240",
						TorPort:  "21749",
					},
					{
						DeviceId: "2",
						DeviceIp: "178.4.196.138",
						TorIp:    "204.254.19.64",
						TorPort:  "17786",
					},
					{
						DeviceId: "3",
						DeviceIp: "30.54.154.194",
						TorIp:    "168.140.225.254",
						TorPort:  "3215",
					},
					{
						DeviceId: "4",
						DeviceIp: "190.174.86.247",
						TorIp:    "146.111.14.186",
						TorPort:  "64140",
					},
					{
						DeviceId: "5",
						DeviceIp: "246.251.52.59",
						TorIp:    "88.121.23.97",
						TorPort:  "39079",
					},
					{
						DeviceId: "6",
						DeviceIp: "158.158.67.96",
						TorIp:    "62.96.59.180",
						TorPort:  "56262",
					},
					{
						DeviceId: "7",
						DeviceIp: "20.27.228.103",
						TorIp:    "143.133.145.191",
						TorPort:  "47173",
					},
				},
			},
			wantErr: nil,
		},
		{
			name: "pod with valid tor annotation",
			annotation: map[string]string{
				PodDeviceAnnotationKey:    "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\"}]}",
				PodRankTableAnnotationKey: "invalid-json",
			},
			devInfo: &PodNpuInfo{
				PodName:  "test-pod",
				ServerId: "101.96.44.5",
				Devices: []NpuDevice{
					{
						DeviceId: "0",
						DeviceIp: "127.126.229.111",
					},
					{
						DeviceId: "1",
						DeviceIp: "159.229.27.93",
					},
					{
						DeviceId: "2",
						DeviceIp: "178.4.196.138",
					},
					{
						DeviceId: "3",
						DeviceIp: "30.54.154.194",
					},
					{
						DeviceId: "4",
						DeviceIp: "190.174.86.247",
					},
					{
						DeviceId: "5",
						DeviceIp: "246.251.52.59",
					},
					{
						DeviceId: "6",
						DeviceIp: "158.158.67.96",
					},
					{
						DeviceId: "7",
						DeviceIp: "20.27.228.103",
					},
				},
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.annotation != nil {
				pod.Annotations = tt.annotation
			}

			devInfo, err := getNpuDevInfo(pod)
			if tt.wantErr != nil && err == nil {
				t.Errorf("Testcase <%s>, want error: <%v>, but got nil", tt.name, tt.wantErr)
			} else if tt.wantErr == nil && err != nil {
				t.Errorf("Testcase <%s>, want nil, but got error: <%v>", tt.name, err)
			} else if tt.wantErr != nil && err != nil {
				if tt.wantErr.Error() != err.Error() {
					t.Errorf("Testcase <%s>, want error: <%v>, but got error: <%v>", tt.name, tt.wantErr, err)
				}
			}

			if tt.devInfo != nil && devInfo == nil {
				t.Errorf("Testcase <%s>, want devInfo: <%v>, but got nil", tt.name, tt.devInfo)
			} else if tt.devInfo != nil && devInfo != nil {
				if !reflect.DeepEqual(*tt.devInfo, *devInfo) {
					t.Errorf("Testcase <%s>, want devInfo: <%+v>, but got devInfo: <%+v>", tt.name, tt.devInfo, devInfo)
				}
			}
		})
	}
}

func TestCacheDevInfo(t *testing.T) {
	var ns *NPUDeviceSyncer
	patch := gomonkey.ApplyPrivateMethod(reflect.TypeOf(ns), "getPodByName", mockGetPodByName)
	defer patch.Reset()

	job := &vcbatchv1.Job{
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
			Tasks: []vcbatchv1.TaskSpec{
				{
					Name:     "worker",
					Replicas: 2,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: "container",
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{
											npuResourceName: resource.MustParse("1"),
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
	mockPodByJob(job)

	tests := []struct {
		name           string
		podName        string
		mockPodBefore  *corev1.Pod
		mockPodAfter   *corev1.Pod
		deleteMockPod  string
		wantEvent      *PodEvent
		wantPodNpuInfo PodDeviceInfo
	}{
		{
			name:    "cached device info",
			podName: "test-job-worker-0",
			mockPodBefore: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "",
					},
				},
			},
			mockPodAfter: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			wantEvent: &PodEvent{
				EventType: EventDeviceCached,
				PodName:   "test-job-worker-0",
			},
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo: PodNpuInfo{
					PodName:  "test-pod",
					ServerId: "101.96.44.5",
					Devices: []NpuDevice{
						{
							DeviceId: "0",
							DeviceIp: "127.126.229.111",
							TorIp:    "247.254.203.180",
							TorPort:  "27010",
						},
						{
							DeviceId: "1",
							DeviceIp: "159.229.27.93",
							TorIp:    "143.64.180.240",
							TorPort:  "21749",
						},
						{
							DeviceId: "2",
							DeviceIp: "178.4.196.138",
							TorIp:    "204.254.19.64",
							TorPort:  "17786",
						},
						{
							DeviceId: "3",
							DeviceIp: "30.54.154.194",
							TorIp:    "168.140.225.254",
							TorPort:  "3215",
						},
						{
							DeviceId: "4",
							DeviceIp: "190.174.86.247",
							TorIp:    "146.111.14.186",
							TorPort:  "64140",
						},
						{
							DeviceId: "5",
							DeviceIp: "246.251.52.59",
							TorIp:    "88.121.23.97",
							TorPort:  "39079",
						},
						{
							DeviceId: "6",
							DeviceIp: "158.158.67.96",
							TorIp:    "62.96.59.180",
							TorPort:  "56262",
						},
						{
							DeviceId: "7",
							DeviceIp: "20.27.228.103",
							TorIp:    "143.133.145.191",
							TorPort:  "47173",
						},
						{
							DeviceId: "8",
							DeviceIp: "83.218.52.8",
							TorIp:    "54.147.170.75",
							TorPort:  "3560",
						},
						{
							DeviceId: "9",
							DeviceIp: "188.87.127.234",
							TorIp:    "234.105.55.184",
							TorPort:  "41237",
						},
						{
							DeviceId: "10",
							DeviceIp: "54.6.152.104",
							TorIp:    "22.119.161.173",
							TorPort:  "51954",
						},
						{
							DeviceId: "11",
							DeviceIp: "205.18.66.191",
							TorIp:    "36.9.191.254",
							TorPort:  "7621",
						},
						{
							DeviceId: "12",
							DeviceIp: "233.149.120.143",
							TorIp:    "106.91.50.146",
							TorPort:  "64589",
						},
						{
							DeviceId: "13",
							DeviceIp: "153.52.51.185",
							TorIp:    "154.90.213.239",
							TorPort:  "23984",
						},
						{
							DeviceId: "14",
							DeviceIp: "18.205.248.88",
							TorIp:    "191.241.16.21",
							TorPort:  "4881",
						},
						{
							DeviceId: "15",
							DeviceIp: "221.30.122.53",
							TorIp:    "234.75.179.217",
							TorPort:  "53818",
						},
					},
				},
				IsCached: true,
			},
		},
		{
			name:    "no change",
			podName: "test-job-worker-0",
			mockPodBefore: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			mockPodAfter: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			wantEvent: nil,
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo: PodNpuInfo{
					PodName:  "test-pod",
					ServerId: "101.96.44.5",
					Devices: []NpuDevice{
						{
							DeviceId: "0",
							DeviceIp: "127.126.229.111",
							TorIp:    "247.254.203.180",
							TorPort:  "27010",
						},
						{
							DeviceId: "1",
							DeviceIp: "159.229.27.93",
							TorIp:    "143.64.180.240",
							TorPort:  "21749",
						},
						{
							DeviceId: "2",
							DeviceIp: "178.4.196.138",
							TorIp:    "204.254.19.64",
							TorPort:  "17786",
						},
						{
							DeviceId: "3",
							DeviceIp: "30.54.154.194",
							TorIp:    "168.140.225.254",
							TorPort:  "3215",
						},
						{
							DeviceId: "4",
							DeviceIp: "190.174.86.247",
							TorIp:    "146.111.14.186",
							TorPort:  "64140",
						},
						{
							DeviceId: "5",
							DeviceIp: "246.251.52.59",
							TorIp:    "88.121.23.97",
							TorPort:  "39079",
						},
						{
							DeviceId: "6",
							DeviceIp: "158.158.67.96",
							TorIp:    "62.96.59.180",
							TorPort:  "56262",
						},
						{
							DeviceId: "7",
							DeviceIp: "20.27.228.103",
							TorIp:    "143.133.145.191",
							TorPort:  "47173",
						},
						{
							DeviceId: "8",
							DeviceIp: "83.218.52.8",
							TorIp:    "54.147.170.75",
							TorPort:  "3560",
						},
						{
							DeviceId: "9",
							DeviceIp: "188.87.127.234",
							TorIp:    "234.105.55.184",
							TorPort:  "41237",
						},
						{
							DeviceId: "10",
							DeviceIp: "54.6.152.104",
							TorIp:    "22.119.161.173",
							TorPort:  "51954",
						},
						{
							DeviceId: "11",
							DeviceIp: "205.18.66.191",
							TorIp:    "36.9.191.254",
							TorPort:  "7621",
						},
						{
							DeviceId: "12",
							DeviceIp: "233.149.120.143",
							TorIp:    "106.91.50.146",
							TorPort:  "64589",
						},
						{
							DeviceId: "13",
							DeviceIp: "153.52.51.185",
							TorIp:    "154.90.213.239",
							TorPort:  "23984",
						},
						{
							DeviceId: "14",
							DeviceIp: "18.205.248.88",
							TorIp:    "191.241.16.21",
							TorPort:  "4881",
						},
						{
							DeviceId: "15",
							DeviceIp: "221.30.122.53",
							TorIp:    "234.75.179.217",
							TorPort:  "53818",
						},
					},
				},
				IsCached: true,
			},
		},
		{
			name:    "clear device info",
			podName: "test-job-worker-0",
			mockPodBefore: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			mockPodAfter: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
				},
			},
			wantEvent: &PodEvent{
				EventType: EventDeviceCleared,
				PodName:   "test-job-worker-0",
			},
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo:  PodNpuInfo{},
				IsCached: false,
			},
		},
		{
			name:    "modify device info",
			podName: "test-job-worker-0",
			mockPodBefore: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			mockPodAfter: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"100.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.100\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			wantEvent: &PodEvent{
				EventType: EventDeviceModified,
				PodName:   "test-job-worker-0",
			},
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo: PodNpuInfo{
					PodName:  "test-pod",
					ServerId: "100.96.44.5",
					Devices: []NpuDevice{
						{
							DeviceId: "0",
							DeviceIp: "127.126.229.111",
							TorIp:    "247.254.203.180",
							TorPort:  "27010",
						},
						{
							DeviceId: "1",
							DeviceIp: "159.229.27.93",
							TorIp:    "143.64.180.240",
							TorPort:  "21749",
						},
						{
							DeviceId: "2",
							DeviceIp: "178.4.196.138",
							TorIp:    "204.254.19.64",
							TorPort:  "17786",
						},
						{
							DeviceId: "3",
							DeviceIp: "30.54.154.100",
							TorIp:    "168.140.225.254",
							TorPort:  "3215",
						},
						{
							DeviceId: "4",
							DeviceIp: "190.174.86.247",
							TorIp:    "146.111.14.186",
							TorPort:  "64140",
						},
						{
							DeviceId: "5",
							DeviceIp: "246.251.52.59",
							TorIp:    "88.121.23.97",
							TorPort:  "39079",
						},
						{
							DeviceId: "6",
							DeviceIp: "158.158.67.96",
							TorIp:    "62.96.59.180",
							TorPort:  "56262",
						},
						{
							DeviceId: "7",
							DeviceIp: "20.27.228.103",
							TorIp:    "143.133.145.191",
							TorPort:  "47173",
						},
						{
							DeviceId: "8",
							DeviceIp: "83.218.52.8",
							TorIp:    "54.147.170.75",
							TorPort:  "3560",
						},
						{
							DeviceId: "9",
							DeviceIp: "188.87.127.234",
							TorIp:    "234.105.55.184",
							TorPort:  "41237",
						},
						{
							DeviceId: "10",
							DeviceIp: "54.6.152.104",
							TorIp:    "22.119.161.173",
							TorPort:  "51954",
						},
						{
							DeviceId: "11",
							DeviceIp: "205.18.66.191",
							TorIp:    "36.9.191.254",
							TorPort:  "7621",
						},
						{
							DeviceId: "12",
							DeviceIp: "233.149.120.143",
							TorIp:    "106.91.50.146",
							TorPort:  "64589",
						},
						{
							DeviceId: "13",
							DeviceIp: "153.52.51.185",
							TorIp:    "154.90.213.239",
							TorPort:  "23984",
						},
						{
							DeviceId: "14",
							DeviceIp: "18.205.248.88",
							TorIp:    "191.241.16.21",
							TorPort:  "4881",
						},
						{
							DeviceId: "15",
							DeviceIp: "221.30.122.53",
							TorIp:    "234.75.179.217",
							TorPort:  "53818",
						},
					},
				},
				IsCached: true,
			},
		},
		{
			name:    "parse error",
			podName: "test-job-worker-0",
			mockPodBefore: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "{\"pod_name\":\"test-pod\",\"server_id\":\"101.96.44.5\",\"devices\":[{\"device_id\":\"14\",\"device_ip\":\"18.205.248.88\",\"tor_ip\":\"191.241.16.21\",\"tor_port\":\"4881\"},{\"device_id\":\"1\",\"device_ip\":\"159.229.27.93\",\"tor_ip\":\"143.64.180.240\",\"tor_port\":\"21749\"},{\"device_id\":\"2\",\"device_ip\":\"178.4.196.138\",\"tor_ip\":\"204.254.19.64\",\"tor_port\":\"17786\"},{\"device_id\":\"3\",\"device_ip\":\"30.54.154.194\",\"tor_ip\":\"168.140.225.254\",\"tor_port\":\"3215\"},{\"device_id\":\"4\",\"device_ip\":\"190.174.86.247\",\"tor_ip\":\"146.111.14.186\",\"tor_port\":\"64140\"},{\"device_id\":\"5\",\"device_ip\":\"246.251.52.59\",\"tor_ip\":\"88.121.23.97\",\"tor_port\":\"39079\"},{\"device_id\":\"6\",\"device_ip\":\"158.158.67.96\",\"tor_ip\":\"62.96.59.180\",\"tor_port\":\"56262\"},{\"device_id\":\"7\",\"device_ip\":\"20.27.228.103\",\"tor_ip\":\"143.133.145.191\",\"tor_port\":\"47173\"},{\"device_id\":\"9\",\"device_ip\":\"188.87.127.234\",\"tor_ip\":\"234.105.55.184\",\"tor_port\":\"41237\"},{\"device_id\":\"10\",\"device_ip\":\"54.6.152.104\",\"tor_ip\":\"22.119.161.173\",\"tor_port\":\"51954\"},{\"device_id\":\"0\",\"device_ip\":\"127.126.229.111\",\"tor_ip\":\"247.254.203.180\",\"tor_port\":\"27010\"},{\"device_id\":\"11\",\"device_ip\":\"205.18.66.191\",\"tor_ip\":\"36.9.191.254\",\"tor_port\":\"7621\"},{\"device_id\":\"12\",\"device_ip\":\"233.149.120.143\",\"tor_ip\":\"106.91.50.146\",\"tor_port\":\"64589\"},{\"device_id\":\"13\",\"device_ip\":\"153.52.51.185\",\"tor_ip\":\"154.90.213.239\",\"tor_port\":\"23984\"},{\"device_id\":\"15\",\"device_ip\":\"221.30.122.53\",\"tor_ip\":\"234.75.179.217\",\"tor_port\":\"53818\"},{\"device_id\":\"8\",\"device_ip\":\"83.218.52.8\",\"tor_ip\":\"54.147.170.75\",\"tor_port\":\"3560\"}]}",
					},
				},
			},
			mockPodAfter: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-worker-0",
					Namespace: "default",
					Annotations: map[string]string{
						PodDeviceAnnotationKey: "invalid-json",
					},
				},
			},
			wantEvent: &PodEvent{
				EventType: EventDeviceParseErr,
				PodName:   "test-job-worker-0",
			},
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo:  PodNpuInfo{},
				IsCached: false,
			},
		},
		{
			name:    "device info not ready",
			podName: "test-job-worker-0",
			mockPodBefore: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-job-worker-0",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
			},
			wantEvent: nil,
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo:  PodNpuInfo{},
				IsCached: false,
			},
		},
		{
			name:      "invalid pod",
			podName:   "invalid pod",
			wantEvent: nil,
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo:  PodNpuInfo{},
				IsCached: false,
			},
		},
		{
			name:          "pod not exist",
			podName:       "test-job-worker-0",
			deleteMockPod: "test-job-worker-0",
			wantEvent:     nil,
			wantPodNpuInfo: PodDeviceInfo{
				DevInfo:  PodNpuInfo{},
				IsCached: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mockPodBefore != nil {
				mockPod(tt.mockPodBefore)
			}

			syncer, _ := NewNPUDevSyncer(nil, job)
			if tt.mockPodBefore != nil {
				mockPod(tt.mockPodBefore)
			}
			syncer.cacheDevInfo(tt.podName)
			if tt.mockPodAfter != nil {
				mockPod(tt.mockPodAfter)
			}
			if tt.deleteMockPod != "" {
				deleteMockPod(tt.deleteMockPod)
			}
			event := syncer.cacheDevInfo(tt.podName)
			if tt.wantEvent == nil && event != nil {
				t.Errorf("Testcase <%s>, want nil, but got event: <%v>", tt.name, event)
			} else if tt.wantEvent != nil && event == nil {
				t.Errorf("Testcase <%s>, want event: <%v>, but got nil", tt.name, tt.wantEvent)
			} else if tt.wantEvent != nil && event != nil {
				if !reflect.DeepEqual(*tt.wantEvent, *event) {
					t.Errorf("Testcase <%s>, want event: <%+v>, but got event: <%+v>", tt.name, tt.wantEvent, event)
				}
			}

			if tt.wantPodNpuInfo.IsCached != syncer.podDeviceInfo[tt.podName].IsCached {
				t.Errorf("Testcase <%s>, want IsCached: <%v>, but got IsCached: <%v>", tt.name, tt.wantPodNpuInfo.IsCached, syncer.podDeviceInfo[tt.podName].IsCached)
			}
			if !reflect.DeepEqual(syncer.podDeviceInfo[tt.podName].DevInfo, tt.wantPodNpuInfo.DevInfo) {
				t.Errorf("Testcase <%s>, want DevInfo: <%+v>, but got DevInfo: <%+v>", tt.name, tt.wantPodNpuInfo.DevInfo, syncer.podDeviceInfo[tt.podName].DevInfo)
			}
		})
	}

}
