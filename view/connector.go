package view

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/scy"
	"sync"
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

		_dsn string
		//TODO add secure password storage
		db          func() (*sql.DB, error)
		initialized bool
		*DBConfig
		mux sync.Mutex
	}

	DBConfig struct {
		MaxIdleConns      int `json:",omitempty" yaml:",omitempty"`
		ConnMaxIdleTimeMs int `json:",omitempty" yaml:",omitempty"`
		MaxOpenConns      int `json:",omitempty" yaml:",omitempty"`
		ConnMaxLifetimeMs int `json:",omitempty" yaml:",omitempty"`
		TimeoutTime       int `json:",omitempty" yaml:",omitempty"`
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

	c._dsn = c.DSN
	c.DSN = ""

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

	if c.DBConfig == nil {
		c.DBConfig = &DBConfig{}
	}

	if err := c.Validate(); err != nil {
		return err
	}

	c.initialized = true
	return nil
}

//DB creates connection to the DB.
//It is important to not close the DB since the connection is shared.
func (c *Connector) DB() (*sql.DB, error) {
	if c.db != nil {
		return c.db()
	}

	var err error
	dsn := c.getDSN()
	var secret *scy.Secret
	if c.Secret != nil {
		secrets := scy.New()
		if secret, err = secrets.Load(context.Background(), c.Secret); err != nil {
			return nil, err
		}
		dsn = secret.Expand(dsn)
	}

	c.mux.Lock()
	c.db = aDbPool.DB(c.Driver, dsn, c.DBConfig)
	aDB, err := c.db()
	c.mux.Unlock()

	return aDB, err
}

//Validate check if connector was configured properly.
//Name, Driver and DSN are required.
func (c *Connector) Validate() error {
	if c.initialized {
		return nil
	}

	if c.Name == "" {
		return fmt.Errorf("connector name was empty")
	}

	if c.Driver == "" {
		return fmt.Errorf("connector driver was empty")
	}

	if FirstNotEmpty(c._dsn, c.DSN) == "" {
		return fmt.Errorf("connector dsn was empty")
	}
	return nil
}

func (c *Connector) inherit(connector *Connector) {
	c._dsn = FirstNotEmpty(c._dsn, c.DSN, connector._dsn, connector.DSN)

	if c.Driver == "" {
		c.Driver = connector.Driver
	}

	if c.db == nil {
		c.db = connector.db
	}

	if c.Name == "" {
		c.Name = connector.Name
	}

	if c.DBConfig == nil {
		c.DBConfig = connector.DBConfig
	}
}

func (c *Connector) setDriverOptions(secret *scy.Secret) {
	if secret == nil || c.initialized {
		return
	}
}

func (c *Connector) getDSN() string {
	return FirstNotEmpty(c._dsn, c.DSN)
}
