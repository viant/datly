package view

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/shared"
	"github.com/viant/scy"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
	"strings"
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

		*DBConfig
		_dialect     *info.Dialect
		_dsn         string
		_db          func() (*sql.DB, error)
		_initialized bool
		_mux         sync.Mutex
		_sharedMux   *sync.Mutex
	}

	ConnectorOption func(c *Connector)

	DBConfig struct {
		MaxIdleConns      int `json:",omitempty" yaml:",omitempty"`
		ConnMaxIdleTimeMs int `json:",omitempty" yaml:",omitempty"`
		MaxOpenConns      int `json:",omitempty" yaml:",omitempty"`
		ConnMaxLifetimeMs int `json:",omitempty" yaml:",omitempty"`
		TimeoutTime       int `json:",omitempty" yaml:",omitempty"`
	}
)

//ConnMaxIdleTime return connector max iddle time
func (c *DBConfig) ConnMaxIdleTime() time.Duration {
	return time.Duration(c.ConnMaxIdleTimeMs) * time.Millisecond
}

//ConnMaxLifetime returns connector max lifetime
func (c *DBConfig) ConnMaxLifetime() time.Duration {
	return time.Duration(c.ConnMaxLifetimeMs) * time.Millisecond
}

//Init initializes connector.
//If Ref is specified, then Connector with the name has to be registered in Connectors
func (c *Connector) Init(ctx context.Context, connectors Connectors) error {
	if c._initialized {
		return nil
	}

	c._dsn = c.DSN
	c.DSN = ""

	if c._initialized {
		return nil
	}

	c._sharedMux = &sync.Mutex{}
	c._initialized = true

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

	c._initialized = true
	return nil
}

//DB creates connection to the DB.
//It is important to not close the DB since the connection is shared.
func (c *Connector) DB() (*sql.DB, error) {
	if c._db != nil {
		return c._db()
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

	c.lock()
	c._db = aDbPool.DB(c.Driver, dsn, c.DBConfig)
	aDB, err := c._db()
	c.unlock()

	return aDB, err
}

func (c *Connector) unlock() {
	if c._sharedMux != nil {
		c._sharedMux.Unlock()
		return
	}

	c._mux.Unlock()
}

func (c *Connector) lock() {
	if c._sharedMux != nil {
		c._sharedMux.Lock()
		return
	}

	c._mux.Lock()
}

//Validate check if connector was configured properly.
//Name, Driver and DSN are required.
func (c *Connector) Validate() error {
	if c._initialized {
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

	if c._db == nil {
		c._db = connector._db
	}

	if c.Name == "" {
		c.Name = connector.Name
	}

	if c.DBConfig == nil {
		c.DBConfig = connector.DBConfig
	}

	if c.Secret == nil {
		c.Secret = connector.Secret
	}
}

func (c *Connector) setDriverOptions(secret *scy.Secret) {
	if secret == nil || c._initialized {
		return
	}
}

func (c *Connector) getDSN() string {
	return FirstNotEmpty(c._dsn, c.DSN)
}

func (c *Connector) Dialect(ctx context.Context) (*info.Dialect, error) {
	if c._dialect != nil {
		return c._dialect, nil
	}

	aDB, err := c.DB()
	if err != nil {
		return nil, err
	}

	dialect, err := config.Dialect(ctx, aDB)
	if err != nil {
		return nil, err
	}

	c._dialect = dialect
	return dialect, nil
}

//EncodedConnector represents encoded connector
type EncodedConnector string

func (c EncodedConnector) Decode() (*Connector, error) {
	parts := strings.Split(string(c), "|")
	if len(parts) < 3 {
		return nil, fmt.Errorf("invalid connector format, expected name|driver|dsn[|secretUrl|key]")
	}
	conn := &Connector{
		Name:   parts[0],
		Driver: parts[1],
		DSN:    parts[2],
	}
	switch len(parts) {
	case 4:
		conn.Secret = &scy.Resource{URL: parts[3]}
	case 5:
		conn.Secret = &scy.Resource{URL: parts[3], Key: parts[4]}
	}
	return conn, nil
}

//DecodeConnectors decodes encoded connectors
func DecodeConnectors(connectors []string) ([]*Connector, error) {
	var result = make([]*Connector, 0)
	for _, conn := range connectors {
		connector, err := EncodedConnector(conn).Decode()
		if err != nil {
			return nil, err
		}
		result = append(result, connector)
	}
	return result, nil
}

func WithSecret(secret *scy.Resource) ConnectorOption {
	return func(c *Connector) {
		c.Secret = secret
	}
}

//NewRefConnector creates connection reference
func NewRefConnector(name string) *Connector {
	return &Connector{Reference: shared.Reference{Ref: name}}
}

//NewConnector creates a connector
func NewConnector(name, driver, dsn string, opts ...ConnectorOption) *Connector {
	ret := &Connector{Name: name, Driver: driver, DSN: dsn}
	for _, opt := range opts {
		opt(ret)
	}
	return ret
}
