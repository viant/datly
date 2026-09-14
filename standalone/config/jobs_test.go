package config_test

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/standalone/config"
	xasync "github.com/viant/xdatly/async"
)

func TestJobsConfigurationValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*config.Config)
	}{
		{"missing jobs", func(c *config.Config) { c.Jobs = nil }},
		{"missing connector", func(c *config.Config) { c.Jobs.Connector = "" }},
		{"blank connector", func(c *config.Config) { c.Jobs.Connector = "   " }},
		{"missing destination", func(c *config.Config) { c.Jobs.Notification.Destination = "" }},
		{"unsupported transport", func(c *config.Config) { c.Jobs.Notification.Method = xasync.NotificationMethodMessageBus }},
		{"missing failed root", func(c *config.Config) { c.FailedJobURL = "" }},
		{"same roots", func(c *config.Config) { c.FailedJobURL = c.JobURL }},
		{"orphan watcher", func(c *config.Config) { c.JobURL = "" }},
		{"negative jobs", func(c *config.Config) { c.MaxJobs = -1 }},
		{"negative ttl", func(c *config.Config) { c.Jobs.TTLSeconds = -1 }},
		{"overflow ttl", func(c *config.Config) { c.Jobs.TTLSeconds = math.MaxInt64 }},
		{"negative error ttl", func(c *config.Config) { c.Jobs.ErrorTTLSeconds = -1 }},
		{"overflow error ttl", func(c *config.Config) { c.Jobs.ErrorTTLSeconds = math.MaxInt64 }},
		{"negative poll", func(c *config.Config) { c.Jobs.PollIntervalMs = -1 }},
		{"overflow poll", func(c *config.Config) { c.Jobs.PollIntervalMs = math.MaxInt64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &config.Config{GoBootstrap: &config.Packages{Packages: []string{"example.com/app"}}, Jobs: &config.Jobs{Connector: "jobs", Notification: xasync.Notification{Method: xasync.NotificationMethodStorage, Destination: "events"}}, JobURL: "events", FailedJobURL: "failed"}
			require.NoError(t, c.Validate())
			tc.change(c)
			require.Error(t, c.Validate())
		})
	}
}

func TestJobsJSONYAMLRelativeConfiguration(t *testing.T) {
	for _, ext := range []string{"json", "yaml"} {
		t.Run(ext, func(t *testing.T) {
			root := t.TempDir()
			name := filepath.Join(root, "config."+ext)
			data := `{"GoBootstrap":{"Packages":["example.com/app"]},"Jobs":{"Connector":"jobs","Table":"APP_JOBS","Dataset":"test","DisableTableCreation":true,"Notification":{"Method":"Storage","Destination":"events"},"TTLSeconds":60,"ErrorTTLSeconds":30,"PollIntervalMs":25},"JobURL":"events","FailedJobURL":"failed","MaxJobs":2}`
			if ext == "yaml" {
				data = "GoBootstrap:\n  Packages: [example.com/app]\nJobs:\n  Connector: jobs\n  Table: APP_JOBS\n  Dataset: test\n  DisableTableCreation: true\n  Notification:\n    Method: Storage\n    Destination: events\n  TTLSeconds: 60\n  ErrorTTLSeconds: 30\n  PollIntervalMs: 25\nJobURL: events\nFailedJobURL: failed\nMaxJobs: 2\n"
			}
			require.NoError(t, os.WriteFile(name, []byte(data), 0600))
			c, err := (config.Loader{}).Load(context.Background(), name)
			require.NoError(t, err)
			require.NoError(t, c.Validate())
			require.Equal(t, c.JobURL, c.Jobs.Notification.Destination)
			require.Contains(t, c.JobURL, root+"/events")
			require.Contains(t, c.FailedJobURL, root+"/failed")
			require.Equal(t, "APP_JOBS", c.Jobs.Table)
			require.Equal(t, "test", c.Jobs.Dataset)
			require.True(t, c.Jobs.DisableTableCreation)
			require.EqualValues(t, 25, c.Jobs.PollIntervalMs)
		})
	}
}
