package bootstrap

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache"
)

func TestCompiledReaderCreatesExecutionWithoutExposingPlan(t *testing.T) {
	type input struct{}
	type output struct{ Rows []struct{} }
	component := &spec.Component{
		Key:      spec.Key{Kind: spec.KindComponent, Scope: "example.com/reader", Name: "Reader"},
		Routes:   []*spec.Route{{Method: "GET", Path: "/reader"}},
		RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT 1"}},
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: component, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	compiled := artifact.ReaderCompilation()
	if compiled == nil {
		t.Fatal("compiled reader is unavailable")
	}
	reader, err := compiled.NewExecution(ReaderRuntimeConfig{SQL: &dsql.SQLComponent{}})
	if err != nil || reader == nil {
		t.Fatalf("NewExecution() = %T, %v", reader, err)
	}
	_, err = compiled.NewExecution(ReaderRuntimeConfig{
		SQL: &dsql.SQLComponent{}, ReadCaches: map[string]cache.Cache{"missing": nil},
	})
	if err == nil || !strings.Contains(err.Error(), `unknown view "missing"`) {
		t.Fatalf("unknown cache view error = %v", err)
	}
}
