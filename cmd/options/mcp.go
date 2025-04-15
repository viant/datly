package options

import (
	"fmt"
)

type Mcp struct {
	Run
	Port *int `short:"p" long:"port" description:"http port"`
}

func (r *Mcp) Init() error {
	if r.ConfigURL == "" {
		return fmt.Errorf("config was empty")
	}
	r.ConfigURL = ensureAbsPath(r.ConfigURL)
	return nil
}
