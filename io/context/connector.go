package context

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/io/db"
)

//Lookup returns connection
func Connector(ctx context.Context, name string, options ...config.Option) (*db.Connector, error) {
	var conn *db.Connector
	if config.Assign(options, &conn) {
		if conn.Name == name {
			return conn, nil
		}
	}
	connectors, err := Connectors(ctx)
	if err != nil {
		return nil, err
	}
	if connectors == nil || connectors.Size() == 0 {
		return nil, fmt.Errorf("connector/s were empty")
	}
	if name == "" && connectors.Size() == 1 {
		return connectors.Connector(connectors.Names()[0])
	}
	return connectors.Connector(name)
}
