package view

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/bigquery"
	"github.com/viant/datly/shared"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/gcp/client"
	"google.golang.org/api/option"
	"time"
)

//Connector represents database/sql named connection config
type (
	Connector struct {
		shared.Reference
		Secret *scy.Resource `json:",omitempty"`
		Name   string        `json:",omitempty"`
		Driver string        `json:",omitempty"`
		DSN    string        `json:",omitempty"`

		//TODO add secure password storage
		db          *sql.DB
		initialized bool
		DBConfig
	}

	DBConfig struct {
		MaxIdleConns      int
		ConnMaxIdleTimeMs int
		MaxOpenConns      int
		ConnMaxLifetimeMs int
	}
)

func (c *DBConfig) ConnMaxIdleTime() time.Duration {
	return time.Duration(c.ConnMaxIdleTimeMs) * time.Millisecond
}

func (c *DBConfig) ConnMaxLifetime() time.Duration {
	return time.Duration(c.ConnMaxLifetimeMs) * time.Millisecond
}

//Init initializes connector.
//If Ref is specified, then Connector with the name has to be registered in Connectors
func (c *Connector) Init(ctx context.Context, connectors Connectors) error {
	if c.initialized {
		return nil
	}

	if c.initialized {
		return nil
	}

	c.initialized = true

	if c.Ref != "" {
		connector, err := connectors.Lookup(c.Ref)
		if err != nil {
			return err
		}
		c.inherit(connector)
	}

	if err := c.Validate(); err != nil {
		return err
	}

	c.initialized = true
	return nil
}

//Db creates connection to the DB.
//It is important to not close the DB since the connection is shared.
func (c *Connector) Db() (*sql.DB, error) {
	if c.db != nil {
		return c.db, nil
	}

	var err error
	dsn := c.DSN
	var secret *scy.Secret
	if c.Secret != nil {
		secrets := scy.New()
		if secret, err = secrets.Load(context.Background(), c.Secret); err != nil {
			return nil, err
		}
		dsn = secret.Expand(dsn)
	}

	if secret != nil {
		c.setDriverOptions(secret)
	}

	c.db, err = aDbPool.DB(c.Driver, dsn, &c.DBConfig)
	return c.db, err
}

//Validate check if connector was configured properly.
//Name, Driver and DSN are required.
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

//Reset reset connector
func (c *Connector) Reset() {
	if c.db == nil {
		return
	}

	_ = c.db.Close()
	c.db = nil
}

//Close closes connector
func (c *Connector) Close() error {
	if c.db == nil {
		return nil
	}
	err := c.db.Close()
	c.db = nil
	return err
}

func (c *Connector) setDriverOptions(secret *scy.Secret) {
	if secret == nil || c.initialized {
		return
	}
	switch c.Driver { //TODO remove globel exposure toward actual database/sql driver
	case "bigquery":
		gcpService := gcp.New(client.NewGCloud())
		client, err := gcpService.AuthClient(context.Background(), append(gcp.Scopes, "https://www.googleapis.com/auth/bigquery")...)
		if err == nil && client != nil {
			bigquery.SetOptions(option.WithHTTPClient(client))
		}
	}
}
