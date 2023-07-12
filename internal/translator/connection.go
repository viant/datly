package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/view"
	"regexp"
	"strconv"
	"strings"
)

var connectorRegex = regexp.MustCompile(`\$DB\[().*\]\.`)
var connectorNameRegex = regexp.MustCompile(`\[().*\]`)

func ExtractConnectorRef(SQL *string) string {
	var connectorRef string
	connectors := connectorRegex.FindAllString(*SQL, -1)
	for _, connector := range connectors {
		connName := strings.Trim(connectorNameRegex.FindString(connector), "[]")
		if unquoted, err := strconv.Unquote(connName); err == nil {
			connName = unquoted
		}
		if connName != "" {
			connectorRef = connName
		}
		*SQL = strings.Replace(*SQL, connector, "", 1)
	}
	return connectorRef
}

func (r *Repository) ensureConnectors(ctx context.Context) (err error) {
	r.NamedConnectors = map[string]*view.Connector{}
	var connectors []*view.Connector
	if encConnectors := r.Config.repository.Connectors; len(encConnectors) > 0 {
		if connectors, err = view.DecodeConnectors(encConnectors); err != nil {
			return err
		}
	}
	view.ConnectorSlice(connectors).IndexInto(&r.NamedConnectors)
	if connectorResource, _ := r.loadDependency(ctx, "connections.yaml"); connectorResource != nil {
		for i, conn := range connectorResource.Connectors {
			if _, ok := r.NamedConnectors[conn.Name]; ok {
				continue
			}
			connectors = append(connectors, connectorResource.Connectors[i])
		}
	}
	if len(connectors) == 0 {
		return fmt.Errorf("connectors were empty")
	}
	r.Connectors = connectors
	view.ConnectorSlice(r.Connectors).IndexInto(&r.NamedConnectors)
	return nil
}

func (r *Repository) loadDependency(ctx context.Context, resourceName string) (*view.Resource, error) {
	URL := url.Join(r.Config.DependencyURL, resourceName)
	if ok, _ := r.fs.Exists(ctx, URL); !ok {
		return nil, nil
	}
	return view.LoadResourceFromURL(ctx, URL, r.fs)
}
