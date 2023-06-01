package options

import (
	"context"
	"github.com/viant/afs/url"
)

type Repository struct {
	Connector
	JwtVerifier
	Repo      string `short:"r" long:"repo" description:"datly rule repository location" `
	Const     string `short:"o" long:"const" description:"const location" `
	Port      int    `short:"P" long:"port" description:"endpoint port" default:"8080"`
	ConfigURL string
}

func (r *Repository) Init(project string) {
	r.Connector.Init()
	r.JwtVerifier.Init()
	expandRelativeIfNeeded(&r.Const, project)
	expandRelativeIfNeeded(&r.Repo, project)
	configURL := url.Join(r.Repo, "Datly/config.json")
	if ok, _ := fs.Exists(context.Background(), configURL); ok {
		r.ConfigURL = configURL
	}
}
