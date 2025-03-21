package configmap1980

import (
	"testing"

	"github.com/stretchr/testify/assert"
	vcbatchv1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

func TestParsePluginArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		expected  *CmArgs
		expectErr bool
	}{
		{
			name: "Valid args with version v1",
			args: []string{"--rank-table-version=v1", "--rank-table-compress=true"},
			expected: &CmArgs{
				RankTableVersion:   "v1",
				RankTableCompress:  true,
				RankTableTorEnable: true,
			},
			expectErr: false,
		},
		{
			name:      "Invalid args",
			args:      []string{"--invalid-flag=true"},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParsePluginArgs(tt.args)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetConfigmapName(t *testing.T) {
	jobName := "test-job"
	expected := "ranktable-test-job"
	result := GetConfigmapName(jobName)
	assert.Equal(t, expected, result)
}

func TestGetTorListCmName(t *testing.T) {
	jobName := "test-job"
	expected := "torlist-test-job"
	result := GetTorListCmName(jobName)
	assert.Equal(t, expected, result)
}

func TestGetPluginArgsByJob(t *testing.T) {
	tests := []struct {
		name      string
		job       *vcbatchv1.Job
		expected  *CmArgs
		expectErr bool
	}{
		{
			name: "Valid job with plugin args",
			job: &vcbatchv1.Job{
				Spec: vcbatchv1.JobSpec{
					Plugins: map[string][]string{
						Name: {"--rank-table-version=v2"},
					},
				},
			},
			expected: &CmArgs{
				RankTableVersion:   "1.0",
				RankTableCompress:  false,
				RankTableTorEnable: true,
			},
			expectErr: false,
		},
		{
			name: "Plugin not found in job",
			job: &vcbatchv1.Job{
				Spec: vcbatchv1.JobSpec{
					Plugins: map[string][]string{},
				},
			},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetPluginArgsByJob(tt.job)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetPluginArgsByHyperJob(t *testing.T) {
	tests := []struct {
		name      string
		hyperJob  *vcbatchv1.HyperJob
		expected  *CmArgs
		expectErr bool
	}{
		{
			name: "Valid hyperjob with plugin args",
			hyperJob: &vcbatchv1.HyperJob{
				Spec: vcbatchv1.HyperJobSpec{
					Plugins: map[string][]string{
						Name: {"--rank-table-version=v2"},
					},
				},
			},
			expected: &CmArgs{
				RankTableVersion:   "1.2",
				RankTableCompress:  false,
				RankTableTorEnable: true,
			},
			expectErr: false,
		},
		{
			name: "Plugin not found in hyperjob",
			hyperJob: &vcbatchv1.HyperJob{
				Spec: vcbatchv1.HyperJobSpec{
					Plugins: map[string][]string{},
				},
			},
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := GetPluginArgsByHyperJob(tt.hyperJob)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
