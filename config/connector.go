package config

import (
	"datly/base"
	"github.com/go-errors/errors"
	"github.com/viant/dsc"
)

//Connector represents database connector
type Connector struct {
	Name               string
	URL                string
	Dialect            string
	SecuredCredentials *Secret
	Config             *dsc.Config
}

//TODO securing credentials
func (c *Connector) Init() error {
	if c.Dialect == "" {
		c.Dialect = base.DialectSQL
	}
	return nil
}

func (c *Connector) Validate() error {
	if c.Config == nil {
		return errors.Errorf("config was empty, %v", c.URL)
	}
	return nil
}
