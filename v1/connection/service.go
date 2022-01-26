package connection

import (
	"database/sql"
	"fmt"
	"github.com/viant/datly/v1/config"
)

type Service struct {
	connectors  config.Connectors
	connections map[string]*sql.DB
}

func (s *Service) Connection(name string) *sql.DB {
	return s.connections[name]
}

//New creates a connection service
func New(connectors ...*config.Connector) (*Service, error) {
	res := &Service{
		connectors:  connectors,
		connections: make(map[string]*sql.DB, len(connectors)),
	}
	for _, item := range connectors {
		db, err := sql.Open(item.Driver, item.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to open connector: %s, %w", item.Name, err)
		}
		res.connections[item.Name] = db
	}
	return res, nil
}
