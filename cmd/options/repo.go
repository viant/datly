package options

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/internal/setter"
	"strings"
)

var defaultPort = 8080

type Repository struct {
	Connector
	Mbus
	JwtVerifier
	Repo                 string `short:"r" long:"repo" description:"datly rule repository location"  default:"repo/dev" `
	ConstURL             string `short:"o" long:"const" description:"const location" `
	Port                 *int   `short:"P" long:"port" description:"endpoint port" `
	APIPrefix            string `short:"a" long:"api" description:"api prefix"  default:"v1/api" `
	ConfigURL            string `short:"C" long:"config" description:"config url" `
	CacheConnectorPrefix string `short:"H" long:"cprefix" description:"cache prefix"`
}

func (r *Repository) Init(ctx context.Context, project string) error {
	if r.Repo == "" && r.ConfigURL != "" {
		if config, _ := standalone.NewConfigFromURL(ctx, r.ConfigURL); config != nil {
			setter.SetStringIfEmpty(&r.APIPrefix, config.APIPrefix)
			setter.SetStringIfEmpty(&r.CacheConnectorPrefix, config.CacheConnectorPrefix)
			if index := strings.LastIndex(r.ConfigURL, "/Datly/"); index != -1 {
				r.Repo = r.ConfigURL[:index]
			}
		}
	}
	if r.Repo == "" {
		return fmt.Errorf("rule repository location was empty")
	}
	if r.APIPrefix == "" {
		r.APIPrefix = "/v1/api"
	}
	r.Connector.Init()
	r.JwtVerifier.Init()
	expandRelativeIfNeeded(&r.Repo, project)
	expandRelativeIfNeeded(&r.ConstURL, project)
	if r.ConfigURL == "" {
		configURL := url.Join(r.Repo, "Datly/config.json")
		if ok, _ := fs.Exists(context.Background(), configURL); ok {
			r.ConfigURL = configURL
		}
	}
	return nil
}
