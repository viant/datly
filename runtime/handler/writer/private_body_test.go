package writer

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

type validationOwnerRow struct {
	ID *int `sqlx:"ID,primaryKey"`
}

type validationOnlyBody struct {
	Submitted string
}

type validationOwnerOutput struct {
	Data []*validationOwnerRow `parameter:"Data,kind=output,in=body"`
}

func TestWriterPrivateValidationBodyKeepsPublicOwner(t *testing.T) {
	for _, privateType := range []reflect.Type{
		reflect.TypeFor[*validationOnlyBody](),
		reflect.TypeFor[[]*validationOnlyBody](),
		reflect.TypeFor[*validationOwnerRow](),
		reflect.TypeFor[[]*validationOwnerRow](),
	} {
		for _, privateFirst := range []bool{false, true} {
			fields := []reflect.StructField{
				{Name: "Rows", Type: reflect.TypeFor[[]*validationOwnerRow](), Tag: `parameter:"Rows,kind=body,in=data" view:"Rows,table=RECORDS"`},
				{Name: "Submitted", Type: privateType, Tag: `parameter:"Submitted,kind=body,in=" internal:"true" json:"-" mcp:"-"`},
			}
			wantIndex := 0
			if privateFirst {
				fields[0], fields[1] = fields[1], fields[0]
				wantIndex = 1
			}
			component := &spec.Component{Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "RECORDS"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true}}}}
			metadata, err := Compile(component, reflect.StructOf(fields), reflect.TypeFor[validationOwnerOutput](), "post")
			if err != nil {
				t.Fatalf("privateFirst=%v: %v", privateFirst, err)
			}
			if metadata.InputField != wantIndex || metadata.EntityType != reflect.TypeFor[validationOwnerRow]() || metadata.Table != "RECORDS" {
				t.Fatalf("privateFirst=%v: private body replaced SQL owner: %+v", privateFirst, metadata)
			}
		}
	}
}

func TestWriterPrivateBodyDoesNotHideInternalWriterRoot(t *testing.T) {
	component := &spec.Component{Settings: &spec.Settings{Mutation: "post"}, Routes: []*spec.Route{{Method: "POST", Path: "/records"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "internal"}, TypeExpr: "[]*validationOwnerRow"}}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "RECORDS"}, Columns: []*spec.Column{{Name: "ID", Source: "ID", PrimaryKey: true}}}}
	input := reflect.StructOf([]reflect.StructField{{Name: "Rows", Type: reflect.TypeFor[[]*validationOwnerRow](), Tag: `parameter:"Rows,kind=internal" internal:"true" view:"Rows,table=RECORDS"`}})
	metadata, err := Compile(component, input, reflect.TypeFor[validationOwnerOutput](), "post")
	if err != nil || metadata.EntityType != reflect.TypeFor[validationOwnerRow]() {
		t.Fatalf("internal writer root lost: metadata=%+v, err=%v", metadata, err)
	}
}

func TestWriterPrivateBodyRequiresStaticInternalDeclaration(t *testing.T) {
	component := &spec.Component{Settings: &spec.Settings{Mutation: "post"}, RootView: &spec.View{Name: "Rows", Source: &spec.ViewSource{Table: "RECORDS"}}}
	for _, visibility := range []string{"", `internal:"false"`, `internal:"True"`, `json:"-" mcp:"-"`} {
		input := reflect.StructOf([]reflect.StructField{
			{Name: "Rows", Type: reflect.TypeFor[[]*validationOwnerRow](), Tag: `parameter:"Rows,kind=body,in=data" view:"Rows,table=RECORDS"`},
			{Name: "Other", Type: reflect.TypeFor[*validationOnlyBody](), Tag: reflect.StructTag(`parameter:"Other,kind=body" ` + visibility)},
		})
		if _, err := Compile(component, input, reflect.TypeFor[validationOwnerOutput](), "post"); err == nil {
			t.Fatalf("nonprivate competing body skipped for %q", visibility)
		}
	}
	privateOnly := reflect.StructOf([]reflect.StructField{{Name: "Submitted", Type: reflect.TypeFor[*validationOnlyBody](), Tag: `parameter:"Submitted,kind=body" internal:"true"`}})
	if _, err := Compile(component, privateOnly, reflect.TypeFor[validationOwnerOutput](), "post"); err == nil {
		t.Fatal("private validation body became sole SQL owner")
	}
}
