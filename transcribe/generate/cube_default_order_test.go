package generate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/report"
	"github.com/viant/datly/spec"
	authoring "github.com/viant/datly/transcribe/compile"
)

type reportDefaultOrderRow struct {
	ID    int     `sqlx:"ID" groupable:"true"`
	Later float64 `sqlx:"later" groupable:"false"`
	FX    float64 `sqlx:"physical_fx|fx" groupable:"false"`
}
type reportDefaultOrderOutput struct {
	Data []*reportDefaultOrderRow `parameter:"Data,kind=output,in=view" view:"records,table=records" sql:"uri=sql/records.sql"`
}

func TestCubeLinkedDefaultOrderPreservesCanonicalScan(t *testing.T) {
	for _, defaultProjection := range []bool{true, false} {
		t.Run(map[bool]string{true: "wildcard linked default", false: "explicit canonical projection"}[defaultProjection], func(t *testing.T) {
			input := linkedPointerMetadataInputFor(t, reflect.TypeFor[reportDefaultOrderRow](), reflect.TypeFor[reportDefaultOrderOutput]())
			input.Component.Settings.Report = &spec.ReportSettings{Enabled: true}
			yes := true
			view := input.Component.RootView
			view.Groupable = &yes
			view.Source.SQL = "SELECT ID, physical_fx AS fx, later FROM records"
			view.Columns = []*spec.Column{
				{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}, Groupable: &yes},
				{Name: "fx", Source: "physical_fx", Output: "fx", Type: spec.TypeRef{Name: "float64"}},
				{Name: "later", Source: "later", Type: spec.TypeRef{Name: "float64"}},
			}
			outer := "SELECT r.ID, r.fx, r.later FROM (" + view.Source.SQL + ") r"
			if defaultProjection {
				outer = "SELECT r.*, CAST(r.fx AS float64) FROM (" + view.Source.SQL + ") r"
			}
			compiled, compileErr := authoring.NewReader().Compile(authoring.ReadInput{View: view, SQL: outer})
			require.NoError(t, compileErr)
			require.Equal(t, defaultProjection, compiled.DefaultProjection)
			input.Component.RootView = compiled
			view = compiled
			require.NoError(t, os.WriteFile(filepath.Join(input.ProjectRoot, "generate", "sql", "records.sql"), []byte(view.Source.SQL), 0600))
			before, err := json.Marshal(input.Component)
			require.NoError(t, err)
			plan, err := New(input).Plan()
			require.NoError(t, err)
			require.Len(t, plan.Cubes, 1)
			cube := plan.Cubes[0]
			wanted := []string{"fx", "later"}
			if defaultProjection {
				wanted = []string{"later", "fx"}
			}
			var names []string
			for _, item := range cube.Definition.Measures {
				names = append(names, item.Name)
			}
			require.Equal(t, wanted, names)
			// The exact emitted definition is the runtime linked-facade authority.
			body, err := json.Marshal(cube.Definition)
			require.NoError(t, err)
			var reloaded report.Definition
			require.NoError(t, json.Unmarshal(body, &reloaded))
			_, err = reloaded.Compile(cube.InputType, reflect.TypeFor[reportDefaultOrderOutput]())
			require.NoError(t, err)
			require.Equal(t, cube.Definition.Measures, reloaded.Measures)
			document, _ := metadataDocument(t, plan)
			var scan []string
			for _, column := range document.Component.RootView.Columns {
				scan = append(scan, column.Name)
			}
			require.Equal(t, []string{"ID", "fx", "later"}, scan)
			after, err := json.Marshal(input.Component)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
		})
	}
}

func TestCubeGeneratedWildcardKeepsSuppliedOrder(t *testing.T) {
	component := cubeFixture()
	component.RootView.DefaultProjection = true
	component.RootView.Source.SQL = "SELECT account_id, SUM(amount) AS amount, SUM(fx) AS fx FROM spend GROUP BY account_id"
	component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "FX", Source: "fx", Type: spec.TypeRef{Name: "float64"}})
	plan, err := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"}).Plan()
	require.NoError(t, err)
	require.Len(t, plan.Cubes, 1)
	require.Equal(t, []string{"amount", "fx"}, []string{plan.Cubes[0].Definition.Measures[0].Name, plan.Cubes[0].Definition.Measures[1].Name})
}
