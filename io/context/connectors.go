package context

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/io/db"
)


//connectorsTypeKey defines context connection key
type connectorsTypeKey string
var _connectorsTypeKey = connectorsTypeKey("connectors")


//WithConnectors returns new context with connectors
func WithConnectors(parent context.Context, connectors ...*config.Connector) (context.Context, error) {
	value := db.NewConnectors()
	for i := range connectors {
		if err := value.Add(connectors[i]); err != nil {
			return nil, err
		}
	}
	return context.WithValue(parent, _connectorsTypeKey, value), nil
}


//Connectors returns connector
func Connectors(ctx context.Context) (*db.Connectors, error) {
	value := ctx.Value(_connectorsTypeKey)
	if value == nil {
		return nil, fmt.Errorf("connectors were nil")
	}
	result, ok := value.(*db.Connectors)
	if !ok { //sanity check: this should never happen
		return nil, fmt.Errorf("invalid connector type %T", value)
	}
	return result, nil
}
