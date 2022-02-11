package config

import "database/sql"

//Connector represents database/sql named connection config
type Connector struct {
	Name   string
	Driver string
	DSN    string
	//TODO add secure password storage

	db *sql.DB
}

func (c *Connector) Db() (*sql.DB, error) {
	if c.db != nil {
		return c.db, nil
	}

	var err error
	c.db, err = sql.Open(c.Driver, c.DSN)
	return c.db, err
}

//Connectors represents list of connector
type Connectors []*Connector
