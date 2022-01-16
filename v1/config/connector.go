package config

//Connector represents database/sql named connection config
type Connector struct {
	Name   string
	Driver string
	DSN    string
	//TODO add secure password storage
}

//Connectors represents list of connector
type Connectors []*Connector
