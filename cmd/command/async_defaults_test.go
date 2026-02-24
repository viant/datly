package command

import (
	"testing"

	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
)

func TestApplyAsyncJobDefaults(t *testing.T) {
	testCases := []struct {
		name         string
		jobURL       string
		failedJobURL string
		expectJob    string
		expectFailed string
	}{
		{
			name:         "both empty use tmp defaults",
			expectJob:    "/tmp/datly/jobs",
			expectFailed: "/tmp/datly/failed",
		},
		{
			name:         "custom job only derives failed path",
			jobURL:       "/custom/jobs",
			expectJob:    "/custom/jobs",
			expectFailed: "file://localhost/custom/failed/jobs",
		},
		{
			name:         "failed only keeps failed and defaults job",
			failedJobURL: "/custom/failed",
			expectJob:    "/tmp/datly/jobs",
			expectFailed: "/custom/failed",
		},
	}

	for _, testCase := range testCases {
		cfg := &standalone.Config{Config: &gateway.Config{}}
		cfg.JobURL = testCase.jobURL
		cfg.FailedJobURL = testCase.failedJobURL

		applyAsyncJobDefaults(cfg)

		if cfg.JobURL != testCase.expectJob {
			t.Fatalf("%s: expected JobURL=%s, got %s", testCase.name, testCase.expectJob, cfg.JobURL)
		}
		if cfg.FailedJobURL != testCase.expectFailed {
			t.Fatalf("%s: expected FailedJobURL=%s, got %s", testCase.name, testCase.expectFailed, cfg.FailedJobURL)
		}
	}
}
