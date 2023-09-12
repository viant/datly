package options

import (
	"fmt"
)

type Run struct {
	ConfigURL  string   `short:"c" long:"conf" description:"datly config"`
	WarmupURIs []string `short:"w" long:"warmup" description:"warmup uris"`
	JobURL     string   `short:"z" long:"joburl" description:"job url"`
}

func (r *Run) Init() error {
	if r.ConfigURL == "" {
		return fmt.Errorf("config was empty")
	}
	r.ConfigURL = ensureAbsPath(r.ConfigURL)
	return nil
}
