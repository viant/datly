package writer

import (
	"reflect"
	"testing"

	"github.com/viant/datly/constant"
	"github.com/viant/datly/spec"
)

type identifierEntity struct {
	ID    *int `sqlx:"id,primaryKey"`
	Owner *int `sqlx:"owner_id,refDb=${schema},refTable=OWNERS${suffix},refColumn=id"`
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
