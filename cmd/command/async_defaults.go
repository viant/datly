package command

import (
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/runtime/standalone"
)

const (
	defaultJobURL       = "/tmp/datly/jobs"
	defaultFailedJobURL = "/tmp/datly/failed"
)

func applyAsyncJobDefaults(config *standalone.Config) {
	if config == nil {
		return
	}

	if config.JobURL == "" && config.FailedJobURL == "" {
		config.JobURL = defaultJobURL
		config.FailedJobURL = defaultFailedJobURL
		return
	}

	if config.JobURL == "" {
		config.JobURL = defaultJobURL
	}

	if config.FailedJobURL == "" {
		parent, _ := url.Split(config.JobURL, file.Scheme)
		config.FailedJobURL = url.Join(parent, "failed", "jobs")
	}
}
