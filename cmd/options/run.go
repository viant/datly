package options

import (
	"fmt"
)

type Run struct {
	ConfigURL    string   `short:"c" long:"conf" description:"datly config"`
	WarmupURIs   []string `short:"w" long:"warmup" description:"warmup uris"`
	JobURL       string   `short:"z" long:"joburl" description:"job url"`
	MaxJobs      int      `short:"W" long:"mjobs" description:"max jobs" default:"40" `
	FailedJobURL string   `short:"F" long:"fjobs" description:"failed jobs" `
	LoadPlugin   bool     `short:"L" long:"lplugin" description:"load plugin"`
	PluginInfo   string
}

func (r *Run) Init() error {
	if r.ConfigURL == "" {
		return fmt.Errorf("config was empty")
	}
	r.ConfigURL = ensureAbsPath(r.ConfigURL)
	return nil
}
