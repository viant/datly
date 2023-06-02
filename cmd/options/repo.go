package options

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
)

type Repository struct {
	Connector
	JwtVerifier
	Repo      string `short:"r" long:"repo" description:"datly rule repository location"  default:"repo/dev" `
	Const     string `short:"o" long:"const" description:"const location" `
	Port      *int   `short:"P" long:"port" description:"endpoint port" `
	ConfigURL string
}

func (r *Repository) Init(project string) error {
	if r.Repo == "" {
		return fmt.Errorf("rule repository location was empty")
	}
	r.Connector.Init()
	r.JwtVerifier.Init()

	expandRelativeIfNeeded(&r.Repo, project)
	expandRelativeIfNeeded(&r.Const, project)
	configURL := url.Join(r.Repo, "Datly/config.json")
	if ok, _ := fs.Exists(context.Background(), configURL); ok {
		r.ConfigURL = configURL
	}
	return nil
}
