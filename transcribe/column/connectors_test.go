package column_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

type tracedConnections struct {
	column.DBResolver
	names []string
}

func (r *tracedConnections) ResolveDB(ctx context.Context, name string) (*sql.DB, error) {
	r.names = append(r.names, name)
	return r.DBResolver.ResolveDB(ctx, name)
}

func discoveryView(name, connection string) *spec.View {
	return &spec.View{Name: name, Source: &spec.ViewSource{
		SQL: "SELECT id, value FROM records", Bindings: &spec.ViewBindings{Connector: connection},
	}}
}

func attach(parent *spec.View, children ...*spec.View) {
	for _, child := range children {
		parent.Relations = append(parent.Relations, &spec.Relation{View: child})
	}
}

func requireValueType(t *testing.T, view *spec.View, want string) {
	t.Helper()
	require.Len(t, view.Columns, 2, view.Name)
	require.Equal(t, "value", view.Columns[1].Name, view.Name)
	require.Equal(t, want, view.Columns[1].Type.Name, view.Name)
	require.NotEmpty(t, view.Columns[1].DatabaseType, "types must come from database discovery")
}

func TestDefaultFreeNamedConnectorDiscovery(t *testing.T) {
	ctx := context.Background()
	var configs []connector.Config
	for _, db := range []struct{ name, typ string }{{"text_db", "TEXT"}, {"number_db", "INTEGER"}} {
		dsn := filepath.Join(t.TempDir(), "records.sqlite")
		h := sqlite.New(t, sqlite.WithDSN(dsn))
		require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL, value "+db.typ+" NOT NULL)"))
		configs = append(configs, connector.Config{Name: db.name, Driver: "sqlite3", DSN: dsn})
	}
	for _, reversed := range []bool{false, true} {
		name := "original_order"
		ordered := append([]connector.Config(nil), configs...)
		if reversed {
			name = "reversed_order"
			ordered[0], ordered[1] = ordered[1], ordered[0]
		}
		t.Run(name, func(t *testing.T) {
			set, err := connector.Open(ctx, ordered, "")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, set.Close()) })
			require.Nil(t, set.SQL.DB, "list order must not select a default")
			_, err = set.ResolveDB(ctx, "")
			require.Error(t, err)

			t.Run("explicit_inherited_and_override", func(t *testing.T) {
				root := discoveryView("root", "text_db")
				inherited := discoveryView("inherited", "")
				override := discoveryView("override", "number_db")
				grandchild := discoveryView("grandchild", "")
				attach(override, grandchild)
				attach(root, inherited, override)
				independent := discoveryView("independent", "number_db")
				component := &spec.Component{Name: "MultiDB", RootView: root, Views: []*spec.View{independent}}
				traced := &tracedConnections{DBResolver: set}
				require.NoError(t, column.New(traced).Refine(ctx, component, nil, nil))
				require.Equal(t, []string{"text_db", "text_db", "number_db", "number_db", "number_db"}, traced.names)
				require.Nil(t, component.Settings)
				requireValueType(t, root, "string")
				requireValueType(t, inherited, "string")
				requireValueType(t, override, "int")
				requireValueType(t, grandchild, "int")
				requireValueType(t, independent, "int")
			})

			t.Run("authored_component_default", func(t *testing.T) {
				root := discoveryView("root", "")
				child := discoveryView("override", "text_db")
				attach(root, child)
				component := &spec.Component{Name: "ExplicitDefault", Settings: &spec.Settings{DefaultConnector: "number_db"}, RootView: root}
				require.NoError(t, column.New(set).Refine(ctx, component, nil, nil))
				requireValueType(t, root, "int")
				requireValueType(t, child, "string")
			})

			t.Run("holders_and_discovery_only_SQL", func(t *testing.T) {
				root := &spec.View{Name: "sql_less_root"}
				holder := &spec.View{Name: "sql_less_holder", InMemory: true}
				placeholder := &spec.View{Name: "signals", InMemory: true, Source: &spec.ViewSource{
					SQL: "SELECT 0 AS id, '' AS value", Bindings: &spec.ViewBindings{Connector: "text_db"},
				}}
				descendant := discoveryView("performance", "number_db")
				inherited := discoveryView("inherited_through_holder", "")
				bridge := &spec.View{Name: "bridge", InMemory: true, Source: &spec.ViewSource{}}
				attach(bridge, inherited)
				attach(placeholder, bridge, descendant)
				attach(holder, placeholder)
				attach(root, holder)
				traced := &tracedConnections{DBResolver: set}
				require.NoError(t, column.New(traced).Refine(ctx, &spec.Component{Name: "HookRows", RootView: root}, nil, nil))
				require.Equal(t, []string{"text_db", "text_db", "number_db"}, traced.names)
				require.Len(t, placeholder.Columns, 2, "in-memory discovery SQL must still execute")
				require.Equal(t, "id", placeholder.Columns[0].Name)
				require.Equal(t, "value", placeholder.Columns[1].Name)
				require.Empty(t, placeholder.RuntimeSource().SQL)
				requireValueType(t, inherited, "string")
				requireValueType(t, descendant, "int")
			})

			t.Run("unknown_does_not_fall_back", func(t *testing.T) {
				traced := &tracedConnections{DBResolver: set}
				component := &spec.Component{Name: "UnknownConnection", Settings: &spec.Settings{DefaultConnector: "text_db"}, RootView: discoveryView("missing", "unselected")}
				err := column.New(traced).Refine(ctx, component, nil, nil)
				require.ErrorContains(t, err, `component "UnknownConnection"`)
				require.ErrorContains(t, err, `view "missing"`)
				require.ErrorContains(t, err, `connector "unselected"`)
				require.Equal(t, []string{"unselected"}, traced.names)
			})

			t.Run("unassigned_does_not_select_first_connection", func(t *testing.T) {
				traced := &tracedConnections{DBResolver: set}
				component := &spec.Component{Name: "Unassigned", RootView: discoveryView("root", "")}
				err := column.New(traced).Refine(ctx, component, nil, nil)
				require.ErrorContains(t, err, `component "Unassigned"`)
				require.ErrorContains(t, err, `view "root"`)
				require.ErrorContains(t, err, "connector is required")
				require.Empty(t, traced.names)
			})

			t.Run("independent_view_does_not_inherit_root_assignment", func(t *testing.T) {
				traced := &tracedConnections{DBResolver: set}
				component := &spec.Component{Name: "Independent", RootView: discoveryView("root", "text_db"), Views: []*spec.View{discoveryView("standalone", "")}}
				err := column.New(traced).Refine(ctx, component, nil, nil)
				require.ErrorContains(t, err, `component "Independent"`)
				require.ErrorContains(t, err, `view "standalone"`)
				require.ErrorContains(t, err, "connector is required")
				require.Equal(t, []string{"text_db"}, traced.names)
			})
		})
	}
}

func TestDiscoveryMissingConnectorContext(t *testing.T) {
	for _, kind := range []string{"root", "descendant", "independent", "in_memory_SQL"} {
		t.Run(kind, func(t *testing.T) {
			missing := discoveryView("unassigned", "")
			component := &spec.Component{Key: spec.Key{Name: "FromKey"}, RootView: &spec.View{Name: "holder"}}
			switch kind {
			case "root":
				component.RootView = missing
			case "independent":
				component.Views = []*spec.View{missing}
			default:
				missing.InMemory = kind == "in_memory_SQL"
				attach(component.RootView, missing)
			}
			traced := &tracedConnections{DBResolver: column.Connections{}}
			err := column.New(traced).Refine(context.Background(), component, nil, nil)
			require.ErrorContains(t, err, `component "FromKey"`)
			require.ErrorContains(t, err, `view "unassigned"`)
			require.ErrorContains(t, err, "connector is required")
			require.Empty(t, traced.names, "do not try a default resolver lookup")
		})
	}
}

type unavailableConnection struct{ err error }

func (r unavailableConnection) ResolveDB(context.Context, string) (*sql.DB, error) {
	return nil, r.err
}

func TestDiscoveryConnectorErrorPreservesCause(t *testing.T) {
	for _, cause := range []error{errors.New("connection unavailable"), context.Canceled, nil} {
		component := &spec.Component{Name: "Unavailable", RootView: &spec.View{
			Key: spec.Key{Name: "FromViewKey"}, Source: &spec.ViewSource{SQL: "SELECT 1", Bindings: &spec.ViewBindings{Connector: "named"}},
		}}
		err := column.New(unavailableConnection{cause}).Refine(context.Background(), component, nil, nil)
		require.ErrorContains(t, err, `component "Unavailable"`)
		require.ErrorContains(t, err, `view "FromViewKey"`)
		require.ErrorContains(t, err, `connector "named"`)
		if cause != nil {
			require.ErrorIs(t, err, cause)
		} else {
			require.ErrorContains(t, err, "nil DB")
		}
	}
}
