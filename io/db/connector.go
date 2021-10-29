package db

import (
	"database/sql"
)

//Connector represents db connector
type Connector struct {
	Name string
	*sql.DB
	*sql.Tx
}

func (c *Connector) Begin() (bool, error) {
	if c.Tx != nil {
		return false, nil
	}
	var err error
	c.Tx, err = c.DB.Begin()
	return true, err
}

func (c *Connector) Commit() error {
	err := c.Tx.Commit()
	c.Tx = nil
	return err
}


func (c *Connector) Rollback() error {
	err := c.Tx.Rollback()
	c.Tx = nil
	return err
}
