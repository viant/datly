package db

import (
	"database/sql"
	"fmt"
	"github.com/viant/datly/config"
	"sync"
)

//Connectors represents connectors
type Connectors struct {
	configs map[string]*config.Connector
	dbs     map[string]*sql.DB
	mux     sync.RWMutex
}

//Size returns connectors size
func (c *Connectors) Size() int {
	return len(c.configs)
}

//Names return connection ids
func (c *Connectors) Names() []string {
	var result = make([]string, 0, len(c.configs))
	for k := range c.configs {
		result = append(result, k)
	}
	return result
}

//Connector returns connector for Name
func (c *Connectors) Connector(name string) (*Connector, error) {
	c.mux.RLock()
	db, ok := c.dbs[name]
	c.mux.RUnlock()
	if ok {
		if err := db.Ping(); err == nil {
			return &Connector{DB: db, Name: name}, nil
		}
	}
	cfg, ok := c.configs[name]
	if !ok {
		return nil, fmt.Errorf("failed to lookup connector for: %v", name)
	}
	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, err
	}
	c.mux.Lock()
	c.dbs[name] = db
	c.mux.Unlock()
	return &Connector{Name: name, DB: db}, nil
}

//Add add connector config
func (c *Connectors) Add(conn *config.Connector) error {
	if err := conn.Validate(); err != nil {
		return err
	}
	c.configs[conn.Name] = conn
	return nil
}

//NewConnectors creates connectors
func NewConnectors() *Connectors {
	connectors := &Connectors{
		configs: make(map[string]*config.Connector),
		dbs:     make(map[string]*sql.DB),
	}
	return connectors
}

