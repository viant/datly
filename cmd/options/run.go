package options

import (
	"fmt"
	"github.com/viant/afs/url"
	"os"
	"path"
)

type Run struct {
	ConfigURL string `short:"c" long:"conf" description:"datly config"`
}

func (r *Run) Init() error {
	if r.ConfigURL == "" {
		return fmt.Errorf("config was empty")
	}
	if url.IsRelative(r.ConfigURL) {
		wd, _ := os.Getwd()
		r.ConfigURL = path.Join(wd, r.ConfigURL)
	}
	return nil
}
