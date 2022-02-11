package config

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/v1/shared"
)

//Connector represents database/sql named connection config
type Connector struct {
	shared.Reference
	Name   string
	Driver string
	DSN    string
	//TODO add secure password storage
	db          *sql.DB
	initialized bool
}

func (c *Connector) Init(ctx context.Context, connectors Connectors) error {
	if c.Ref != "" {
		connector, err := connectors.Lookup(c.Ref)
		if err != nil {
			return err
		}
		c.inherit(connector)
	}

	if c.Driver == "" {
		return fmt.Errorf("connector driver was empty")
	}

	if c.DSN == "" {
		return fmt.Errorf("connector dsn was empty")
	}

	db, err := c.Db()
	if err != nil {
		return err
	}

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	c.initialized = true
	return nil
}

func (c *Connector) Db() (*sql.DB, error) {
	if c.db != nil {
		return c.db, nil
	}

	var err error
	c.db, err = sql.Open(c.Driver, c.DSN)
	return c.db, err
}

func (c *Connector) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("connector name was empty")
	}

	if c.Driver == "" {
		return fmt.Errorf("connector driver was empty")
	}

	if c.DSN == "" {
		return fmt.Errorf("connector dsn was empty")
	}
	return nil
}

func (c *Connector) inherit(connector *Connector) {
	if c.DSN == "" {
		c.DSN = connector.DSN
	}

	if c.Driver == "" {
		c.Driver = connector.Driver
	}

	if c.DSN == "" {
		c.DSN = connector.DSN
	}

	if c.db == nil {
		c.db = connector.db
	}

	if c.Name == "" {
		c.Name = connector.Name
	}
}
