package config

import (
	"github.com/go-errors/errors"
	"github.com/viant/datly/shared"
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

//Init initialise connector
func (c *Connector) Init() error {
	if c.Dialect == "" {
		c.Dialect = shared.DialectSQL
	}
	return nil
}

//Validate checks if connector is valid
func (c *Connector) Validate() error {
	if c.Config == nil {
		return errors.Errorf("config was empty, %v", c.URL)
	}
	return nil
}
