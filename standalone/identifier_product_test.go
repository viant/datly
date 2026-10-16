package standalone

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/registry"
)

type metadataProductDriver struct{ opens atomic.Int32 }

func (d *metadataProductDriver) Open(string) (driver.Conn, error) {
	d.opens.Add(1)
	return nil, fmt.Errorf("metadata compilation must not open a database")
}

type metadataProductConnector struct{ driver *metadataProductDriver }

func (c metadataProductConnector) Connect(context.Context) (driver.Conn, error) {
	return c.driver.Open("")
}
func (c metadataProductConnector) Driver() driver.Driver { return c.driver }

type metadataProductInput struct{}
type metadataProductRow struct {
	ID int `sqlx:"id"`
}
type metadataProductOutput struct {
	Data []*metadataProductRow `parameter:"Data,kind=output,in=body" view:"records"`
}

func TestArtifactPathsCarryIdentifierProductWithoutDatabaseIO(t *testing.T) {
	typ := reflect.TypeFor[metadataProductDriver]()
	require.NoError(t, registry.RegisterDriver(database.Product{Name: "BigQuery"}, typ.PkgPath(), typ.Name()))
	driver := &metadataProductDriver{}
	db := sql.OpenDB(metadataProductConnector{driver: driver})
	t.Cleanup(func() { _ = db.Close() })
	connections := &connector.Set{SQL: &dsql.SQLComponent{}}
	require.NoError(t, connections.SQL.RegisterConnector("bq_mdp", db))
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Name: "Emissions"}, Routes: []*spec.Route{{Method: "GET", Path: "/emissions"}},
		Settings: &spec.Settings{DefaultConnector: "bq_mdp", InputType: "metadataProductInput", OutputType: "metadataProductOutput", Const: map[string]string{"project": "authored-project"}},
		RootView: &spec.View{Name: "records", Source: &spec.ViewSource{Table: "${project}.scope3.emissions_data", SQL: "SELECT id FROM `${project}.scope3.emissions_data`"}},
	}
	for _, project := range []string{"viant-e2e", "viant-prod"} {
		t.Run(project, func(t *testing.T) {
			t.Parallel()
			values, err := constant.New(map[string]string{"project": project})
			require.NoError(t, err)
			compiler := &sourceComponent{source: &source{config: &config.Config{Const: values}, connections: connections}}
			for _, mode := range []string{"reflected", "source"} {
				var input bootstrap.ArtifactInput
				if mode == "reflected" {
					input, err = compiler.reflectedArtifactInput(component, &bootstrap.RouteSource{LinkedInputType: reflect.TypeFor[metadataProductInput](), LinkedOutputType: reflect.TypeFor[metadataProductOutput]()}, typecatalog.NewCatalog(), resource.New())
				} else {
					input, err = compiler.artifactInput(&transcribe.Result{Component: component, Source: &transcribe.Source{LinkedInputType: reflect.TypeFor[metadataProductInput](), LinkedOutputType: reflect.TypeFor[metadataProductOutput](), Resources: resource.New(), Types: typecatalog.NewCatalog()}})
				}
				require.NoError(t, err, mode)
				artifact, err := bootstrap.BuildArtifact(input)
				require.NoError(t, err, mode)
				require.Equal(t, project+".scope3.emissions_data", artifact.Component.RootView.Source.Table)
				require.Equal(t, component.RootView.Source.SQL, artifact.Component.RootView.Source.SQL)
			}
			require.Equal(t, "${project}.scope3.emissions_data", component.RootView.Source.Table)
			require.Zero(t, driver.opens.Load())
		})
	}
}
