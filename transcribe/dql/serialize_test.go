package dql_test

import (
	"context"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/dql"
	"testing"
)

func TestReverseDQLSupportedProjectionRoundtrip(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Read"}, Routes: []*spec.Route{{Path: "/read", Method: "GET"}}, Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Source: &spec.ViewSource{SQL: "SELECT 1 AS id"}}}
	reversed := (dql.Serializer{}).Export(component, "")
	if !reversed.Supported || reversed.Original {
		t.Fatalf("%+v", reversed)
	}
	compiled, err := transcribe.NewCompiler().Compile(context.Background(), &transcribe.Source{Scope: component.Key.Scope, Name: component.Key.Name, Text: reversed.Source})
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Component.Routes[0].Path != "/read" || compiled.Component.Settings.DefaultConnector != "main" || compiled.Component.RootView == nil {
		t.Fatal("supported projection changed")
	}
	component.Routes[0].Handler = "OpaqueGo"
	if result := (dql.Serializer{}).Export(component, ""); result.Supported || len(result.Limitations) == 0 || result.Source != "" {
		t.Fatal("opaque behavior invented")
	}
	if result := (dql.Serializer{}).Export(component, "original bytes\n"); !result.Original || result.Source != "original bytes\n" {
		t.Fatal("retained source changed")
	}
}
