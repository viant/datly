package config

import (
	"fmt"
	"strings"
	"time"

	afsurl "github.com/viant/afs/url"
	xasync "github.com/viant/xdatly/async"
)

// Jobs selects the existing application job services. Authorization is supplied
// by the linked standalone host, never inferred from serialized configuration.
// Connector/table selection is fixed for the server's lifetime.
type Jobs struct {
	Connector            string
	Table                string
	Dataset              string
	DisableTableCreation bool
	Notification         xasync.Notification
	TTLSeconds           int64
	ErrorTTLSeconds      int64
	PollIntervalMs       int64
}

func (c *Config) validateJobs() error {
	if c.Jobs == nil {
		if c.JobURL != "" || c.FailedJobURL != "" || c.MaxJobs != 0 || len(c.Async) != 0 {
			return fmt.Errorf("standalone async requires Jobs configuration and a linked Async authorizer")
		}
		return nil
	}
	j := c.Jobs
	if strings.TrimSpace(j.Connector) == "" {
		return fmt.Errorf("Jobs.Connector is required")
	}
	if j.Notification.Method != xasync.NotificationMethodStorage || j.Notification.Destination == "" {
		return fmt.Errorf("Jobs.Notification requires Method Storage and Destination")
	}
	const maxSeconds = int64((1<<63 - 1) / int64(time.Second))
	const maxMs = int64((1<<63 - 1) / int64(time.Millisecond))
	if j.TTLSeconds < 0 || j.TTLSeconds > maxSeconds || j.ErrorTTLSeconds < 0 || j.ErrorTTLSeconds > maxSeconds || j.PollIntervalMs < 0 || j.PollIntervalMs > maxMs || c.MaxJobs < 0 {
		return fmt.Errorf("invalid async retention or watcher limits")
	}
	if c.JobURL == "" {
		if c.FailedJobURL != "" || c.MaxJobs != 0 || j.PollIntervalMs != 0 {
			return fmt.Errorf("async watcher settings require JobURL")
		}
	} else if c.FailedJobURL == "" || afsurl.Equals(c.JobURL, c.FailedJobURL) {
		return fmt.Errorf("JobURL requires a distinct FailedJobURL")
	}
	return nil
}
