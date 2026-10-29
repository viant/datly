package report

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	rhandler "github.com/viant/datly/runtime/handler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestPrepareCompositionUsesRegisteredNativeHandlerAndExactSourceRoute(t *testing.T) {
	input := reportArtifactInput(t, &spec.ReportSettings{Enabled: true, Compose: &spec.CubeComposeSettings{Enabled: true, MaxCubes: 2, MaxLimit: 20}})
	input.Component.Parameters = append(input.Component.Parameters, &spec.Parameter{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}})
	input.Component.Routes = []*spec.Route{{Method: "GET", Path: "/records/403", Name: "Events"}, {Method: "GET", Path: "/records/408", Name: "Events"}}
	compiled, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).CompileArtifacts([]bootstrap.ArtifactInput{input})
	if err != nil {
		t.Fatal(err)
	}
	registered, err := compiled.RuntimeComponents(context.Background(), RuntimeConfigureFunc(func(_ context.Context, artifact *ComponentArtifact) (RuntimeCapabilities, error) {
		if !artifact.IsReport() {
			return RuntimeCapabilities{Handler: customhandler.NewFunc[reportSourceInput, reportSourceOutput](func(_ context.Context, _ *reportSourceInput) (*reportSourceOutput, error) {
				return &reportSourceOutput{}, nil
			})}, nil
		}
		return RuntimeCapabilities{}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	var sourceRegistration = registered[0]
	recompiled, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).CompileArtifacts([]bootstrap.ArtifactInput{input})
	if err != nil {
		t.Fatal(err)
	}
	stale := *sourceRegistration
	stale.Input = recompiled.Artifacts()[0].InputContract()
	seen := map[string]bool{}
	for _, component := range registered {
		source, native := CompositionSource(component.Handler)
		if !native {
			continue
		}
		if source.Route.Method != "GET" || source.Component != input.Component.Key || component.Component.Routes[0].Path != source.Route.Path+"/cube/compose" {
			t.Fatalf("native provenance differs: %+v %+v", source, component.Component.Routes)
		}
		seen[source.Route.Path] = true
		if !CompositionMatchesSource(component.Handler, sourceRegistration) || CompositionMatchesSource(component.Handler, &stale) {
			t.Fatal("equal route identities bypassed compiled generation provenance")
		}
		end := 5
		definition := CompositionDefinition{SQL: "SELECT t1.AccountID AS account,t1.TotalSpend AS amount FROM $CubeSQL1 AS t1 ORDER BY amount DESC LIMIT 10", FrameCount: 1, ResultLimit: &end}
		prepared, err := PrepareComposition(component.Handler, definition)
		if err != nil {
			t.Fatal(err)
		}
		if prepared.SourceTarget() != source || len(prepared.Columns()) != 2 || prepared.Columns()[1].RuntimeType() != reflect.TypeFor[float64]() || prepared.Budget().MaxRows != 20 {
			t.Fatalf("native preparation contract differs: %+v", prepared)
		}
		filters := prepared.Filters()
		if len(filters) != 1 || filters[0].Name != "accountIds" || filters[0].Parameter != "AccountIDs" || filters[0].Type != reflect.TypeFor[[]int]() {
			t.Fatalf("source filter contract differs: %+v", filters)
		}
		columns := prepared.Columns()
		columns[0].Name = "changed"
		filters[0].Name = "changed"
		budget := prepared.Budget()
		budget.MaxRows = 1000
		if prepared.Columns()[0].Name != "account" || prepared.Filters()[0].Name != "accountIds" || prepared.Budget().MaxRows != 20 {
			t.Fatal("returned metadata aliases native preparation")
		}
		for _, bad := range []CompositionDefinition{
			{SQL: strings.Replace(definition.SQL, "TotalSpend", "Unknown", 1), FrameCount: 1},
			{SQL: definition.SQL + "; DELETE FROM records", FrameCount: 1},
			{SQL: definition.SQL, FrameCount: 0},
			{SQL: definition.SQL, FrameCount: 3},
		} {
			if _, err = PrepareComposition(component.Handler, bad); err == nil {
				t.Fatalf("native invalid definition admitted: %+v", bad)
			}
		}
		if _, err = PrepareComposition(&opaqueCompositionWrapper{component.Handler}, definition); err == nil {
			t.Fatal("opaque handler wrapper gained native provenance")
		}
	}
	if len(seen) != 2 || !seen["/records/403"] || !seen["/records/408"] {
		t.Fatalf("multi-route provenance incomplete: %+v", seen)
	}
	if _, err = PrepareComposition(nil, CompositionDefinition{}); err == nil {
		t.Fatal("nil handler admitted")
	}
}

type opaqueCompositionWrapper struct{ rhandler.Handler }
