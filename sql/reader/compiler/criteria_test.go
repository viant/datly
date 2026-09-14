package compiler

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/builder"
	xstate "github.com/viant/xdatly/state"
)

func TestBuildArtifactTypedCriteriaSQLite(t *testing.T) {
	type record struct {
		ID    int8      `sqlx:"id"`
		Name  string    `sqlx:"name"`
		Stamp time.Time `sqlx:"stamp" format:"dateFormat=YYYY-MM-DD"`
	}
	type output struct{ Data []record }
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT,stamp TIMESTAMP)", "INSERT INTO records VALUES(1,'ALICE','2026-09-12 00:00:00+00:00')"); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		RootView:   &spec.View{Source: &spec.ViewSource{SQL: "SELECT id,name,stamp FROM records"}, Selector: &spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"*"}, SQLMethods: []spec.SQLMethod{{Name: "upper", Args: []string{"string"}}}}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := BuildArtifact(ArtifactInput{Component: component, OutputType: reflect.TypeOf(output{}), DirectViewField: "Data"})
	if err != nil {
		t.Fatal(err)
	}
	root := artifact.Reader.Root
	for _, tt := range []struct {
		name, source string
		arg          any
		reject       bool
	}{
		{"integer", "id=1", int8(1), false},
		{"time", "stamp='2026-09-12'", time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC), false},
		{"method", "name=upper('alice')", "alice", false},
		{"overflow", "id=128", nil, true},
		{"unknown method", "name=lower('ALICE')", nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			query, err := builder.NewBuilder().Build(ctx, builder.WithBuilderView(root.View), builder.WithBuilderCriteriaCompiler(root.Criteria), builder.WithBuilderSQL("SELECT id FROM records"), builder.WithBuilderSelector(&xstate.Selector{Criteria: tt.source}))
			if tt.reject {
				if err == nil {
					t.Fatal("expected rejection")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(query.Args) != 1 || !reflect.DeepEqual(query.Args[0], tt.arg) {
				t.Fatalf("args %#v expected %#v", query.Args, tt.arg)
			}
			type row struct {
				ID int `sqlx:"id"`
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query.SQL, Args: query.Args}, []row{{ID: 1}})
		})
	}
}
