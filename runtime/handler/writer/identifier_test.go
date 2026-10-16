package writer

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/constant/identifier"
	"github.com/viant/datly/spec"
)

type identifierEntity struct {
	ID    *int `sqlx:"id,primaryKey"`
	Owner *int `sqlx:"owner_id,refDb=${schema},refTable=OWNERS${suffix},refColumn=id"`
}

type productWriterEntity struct {
	ID    *int `sqlx:"id,primaryKey"`
	Owner *int `sqlx:"owner_id,refDb=${project},refTable=${project}.automation.owners,refColumn=id"`
}

type productWriterInput struct {
	Rows []*productWriterEntity `parameter:"Rows,kind=body,in=data" view:"Rows,table=$Unsafe.Table"`
}

type productWriterOutput struct {
	Data []*productWriterEntity `parameter:"Data,kind=output,in=body"`
}

func TestWriterIdentifierProduct(t *testing.T) {
	component := &spec.Component{Settings: &spec.Settings{Mutation: "post", DefaultConnector: "metrics", Const: map[string]string{"project": "viant-e2e", "Table": "viant-e2e.scope3.emissions_data"}}, RootView: &spec.View{
		Name: "Rows", Source: &spec.ViewSource{Table: "$Unsafe.Table"},
		Columns: []*spec.Column{{Name: "id", PrimaryKey: true}, {Name: "owner_id"}},
	}}
	products := func(connector string) (string, error) {
		require.Equal(t, "metrics", connector)
		return "BigQuery", nil
	}
	metadata, err := CompileWithIdentifierProducts(component, reflect.TypeFor[productWriterInput](), reflect.TypeFor[productWriterOutput](), "post", products)
	require.NoError(t, err)
	require.Equal(t, "viant-e2e.scope3.emissions_data", metadata.Table)
	require.Equal(t, "viant-e2e.automation.owners", metadata.Fields[1].RefTable)
	require.Equal(t, "viant-e2e", metadata.Fields[1].RefDB)
	require.Equal(t, reflect.TypeFor[productWriterEntity](), metadata.EntityType)
	values, err := (*constant.Values)(nil).For(component)
	require.NoError(t, err)
	table, err := resolvedWriterTable(component, "`${project}.scope3.emissions_data`", identifier.Renderer{Values: values, Product: "BigQuery"})
	require.NoError(t, err)
	require.Equal(t, "`viant-e2e.scope3.emissions_data`", table)
	_, err = Compile(component, reflect.TypeFor[productWriterInput](), reflect.TypeFor[productWriterOutput](), "post")
	require.Error(t, err)
	require.Equal(t, "$Unsafe.Table", component.RootView.Source.Table)
}

type identifierInput struct {
	Rows []*identifierEntity `parameter:"Rows,kind=body,in=data" view:"Rows,table=RECORDS${suffix}"`
}

type identifierOutput struct {
	Data []*identifierEntity `parameter:"Data,kind=output,in=body"`
}

func TestWriterIdentifiersResolveWithoutChangingContract(t *testing.T) {
	component := &spec.Component{Settings: &spec.Settings{Mutation: "post", Const: map[string]string{"schema": "main", "suffix": "_DEFAULT"}}, RootView: &spec.View{
		Name: "Rows", Source: &spec.ViewSource{Table: "RECORDS${suffix}"},
		Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true}, {Name: "owner_id", Source: "owner_id"}},
	}}
	for _, suffix := range []string{"_SOAK", "_PROD"} {
		t.Run(suffix, func(t *testing.T) {
			t.Parallel()
			values, err := constant.New(map[string]string{"schema": "main", "suffix": suffix})
			if err != nil {
				t.Fatal(err)
			}
			instance := values.Apply(component)
			metadata, err := Compile(instance, reflect.TypeFor[identifierInput](), reflect.TypeFor[identifierOutput](), "post")
			if err != nil {
				t.Fatal(err)
			}
			if metadata.Table != "RECORDS"+suffix || metadata.EntityType != reflect.TypeFor[identifierEntity]() {
				t.Fatalf("writer table/type: %s %s", metadata.Table, metadata.EntityType)
			}
			owner := metadata.Fields[1]
			if owner.RefDB != "main" || owner.RefTable != "OWNERS"+suffix {
				t.Fatalf("writer reference: %+v", owner)
			}
			if component.RootView.Source.Table != "RECORDS${suffix}" || reflect.TypeFor[identifierEntity]().Field(1).Tag.Get("sqlx") != "owner_id,refDb=${schema},refTable=OWNERS${suffix},refColumn=id" {
				t.Fatal("authored metadata changed")
			}
		})
	}
}
