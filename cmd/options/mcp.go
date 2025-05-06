package options

import (
	"fmt"
)

type Mcp struct {
	Run
	Port            *int   `short:"p" long:"port" description:"http port"`
	OAuth2ConfigURL string `short:"C" long:"authclient" description:"auth client url"`
	IssuerURL       string `short:"I" long:"issuerurl" description:"issuer url"`
	Authorizer      string `short:"A" long:"auth" description:"authorizer S - server authorizer, F fallback authorizer (server size)" choice:"F" choice:"S"`
}

func (r *Mcp) Init() error {
	if r.ConfigURL == "" {
		return fmt.Errorf("config was empty")
	}
	r.ConfigURL = ensureAbsPath(r.ConfigURL)
	return nil
}
