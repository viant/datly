package gateway

import "testing"

func TestValidateAsyncJobPaths(t *testing.T) {
	testCases := []struct {
		name         string
		jobURL       string
		failedJobURL string
		wantErr      bool
	}{
		{name: "empty job url allowed", jobURL: "", failedJobURL: "", wantErr: false},
		{name: "default tmp datly jobs allowed", jobURL: "/tmp/datly/jobs", failedJobURL: "/tmp/datly/failed", wantErr: false},
		{name: "file tmp datly jobs allowed", jobURL: "file://localhost/tmp/datly/jobs", failedJobURL: "file://localhost/tmp/datly/failed", wantErr: false},
		{name: "tmp root rejected", jobURL: "/tmp", failedJobURL: "/tmp/datly/failed", wantErr: true},
		{name: "filesystem root rejected", jobURL: "/", failedJobURL: "/tmp/datly/failed", wantErr: true},
		{name: "file tmp root rejected", jobURL: "file://localhost/tmp", failedJobURL: "file://localhost/tmp/datly/failed", wantErr: true},
		{name: "failed path root rejected", jobURL: "/tmp/datly/jobs", failedJobURL: "/", wantErr: true},
	}

	for _, testCase := range testCases {
		err := validateAsyncJobPaths(testCase.jobURL, testCase.failedJobURL)
		if testCase.wantErr && err == nil {
			t.Fatalf("%s: expected error", testCase.name)
		}
		if !testCase.wantErr && err != nil {
			t.Fatalf("%s: unexpected error: %v", testCase.name, err)
		}
	}
}
