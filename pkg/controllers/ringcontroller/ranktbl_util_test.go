package ringcontroller

import (
	"testing"
)

func TestSyncEvent_GetKey(t *testing.T) {
	testCases := []struct {
		name        string
		event       SyncEvent
		expectedKey string
	}{
		{
			name: "Normal case",
			event: SyncEvent{
				JobType:   "job",
				Namespace: "default",
				Name:      "test-job",
			},
			expectedKey: "job/default/test-job",
		},
		{
			name: "Empty namespace",
			event: SyncEvent{
				JobType: "hyperJob",
				Name:    "test",
			},
			expectedKey: "hyperJob//test",
		},
		{
			name: "Empty name",
			event: SyncEvent{
				JobType:   "job",
				Namespace: "ns",
			},
			expectedKey: "job/ns/",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := tc.event.GetKey()
			if key != tc.expectedKey {
				t.Errorf("GetKey() = %q, want %q", key, tc.expectedKey)
			}
		})
	}
}

func TestGetSyncerName(t *testing.T) {
	testCases := []struct {
		testCaseName string
		jobType      string
		namespace    string
		name         string
		expected     string
		expectPanic  bool
	}{
		{
			testCaseName: "Normal case",
			jobType:      "job",
			namespace:    "default",
			name:         "test-job",
			expected:     "job/default/test-job",
		},
		{
			testCaseName: "Empty namespace",
			jobType:      "hyperJob",
			namespace:    "",
			name:         "test",
			expected:     "hyperJob//test",
		},
		{
			testCaseName: "Empty jobType",
			jobType:      "",
			namespace:    "ns",
			name:         "name",
			expected:     "/ns/name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testCaseName, func(t *testing.T) {
			result := GetSyncerName(tc.jobType, tc.namespace, tc.name)
			if result != tc.expected {
				t.Errorf("GetSyncerName() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestGetPodName(t *testing.T) {
	testCases := []struct {
		name        string
		jobName     string
		taskName    string
		taskIndex   int
		expected    string
		expectPanic bool
	}{
		{
			name:      "Normal case",
			jobName:   "myjob",
			taskName:  "task1",
			taskIndex: 0,
			expected:  "myjob-task1-0",
		},
		{
			name:      "Negative index",
			jobName:   "job",
			taskName:  "task",
			taskIndex: -1,
			expected:  "job-task--1",
		},
		{
			name:      "Empty taskName",
			jobName:   "job",
			taskName:  "",
			taskIndex: 5,
			expected:  "job--5",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetPodName(tc.jobName, tc.taskName, tc.taskIndex)
			if result != tc.expected {
				t.Errorf("GetPodName() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestGetRjName(t *testing.T) {
	testCases := []struct {
		name              string
		hyperJobName      string
		replicatedJobName string
		rjIndex           int
		expected          string
	}{
		{
			name:              "Normal case",
			hyperJobName:      "hyperjob",
			replicatedJobName: "repjob",
			rjIndex:           2,
			expected:          "hyperjob-repjob-2",
		},
		{
			name:              "Negative index",
			hyperJobName:      "test",
			replicatedJobName: "rep",
			rjIndex:           -3,
			expected:          "test-rep--3",
		},
		{
			name:              "Empty replicatedJobName",
			hyperJobName:      "hjob",
			replicatedJobName: "",
			rjIndex:           0,
			expected:          "hjob--0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetRjName(tc.hyperJobName, tc.replicatedJobName, tc.rjIndex)
			if result != tc.expected {
				t.Errorf("GetRjName() = %q, want %q", result, tc.expected)
			}
		})
	}
}
