package runtime

import (
	"context"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	custom "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"
)

func TestRuntimeAuxiliaryRecorderIsolationSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE users(id INTEGER PRIMARY KEY,tenant INTEGER,name TEXT)", "INSERT INTO users VALUES(1,7,'one'),(2,8,'other')"); err != nil {
		t.Fatal(err)
	}
	component := independentViewComponent("Observed", spec.CardinalityMany)
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[independentManyInput](), OutputType: reflect.TypeFor[independentViewOutput]()})
	if err != nil {
		t.Fatal(err)
	}
	provided, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	registered := &registry.RegisteredComponent{Component: component, Input: artifact.Input, OutputType: reflect.TypeFor[independentViewOutput](), Providers: []locator.Provider{provided}, Handler: custom.NewFunc(func(_ context.Context, input *independentManyInput) (*independentViewOutput, error) {
		result := &independentViewOutput{}
		for _, row := range input.Existing {
			result.IDs = append(result.IDs, row.ID)
		}
		return result, nil
	})}
	counts := [2]int{}
	create := func(index int) *Runtime {
		rt, err := NewRuntime([]*RegisteredComponent{registered}, WithObservability(ObservabilityConfig{ReadingData: func(_ string, _ time.Duration, _ string, rows int, args []any, err error) {
			if err != nil || rows != 1 || len(args) != 1 {
				t.Errorf("unexpected native observation rows%d args%v err%v", rows, args, err)
			}
			counts[index]++
		}}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = rt.Shutdown(ctx) })
		return rt
	}
	a, b := create(0), create(1)
	for _, rt := range []*Runtime{a, b, a} {
		value, err := executeTestRoute(t, rt, ctx, testharness.NewRequest(http.MethodGet, "/views").WithQuery(url.Values{"tenant": {"7"}}))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(value.(*independentViewOutput).IDs, []int{1}) {
			t.Fatalf("typed scoped query changed: %+v", value)
		}
	}
	if counts != [2]int{2, 1} {
		t.Fatalf("runtime observation missing or cross-contaminated: %v", counts)
	}
}
