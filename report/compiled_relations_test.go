package report

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
)

type compiledRelationRow struct {
	Country string                  `sqlx:"country" groupable:"true"`
	Region  string                  `sqlx:"region" groupable:"true"`
	Amount  int                     `sqlx:"amount"`
	Lookup  *compiledRelationLookup `on:"Country:v.country=Country:country,Region:v.region=Region:region" sql:"SELECT country, region, label FROM lookup"`
}
type compiledRelationLookup struct{ Country, Region, Label string }
type compiledRelationOutput struct {
	Rows []compiledRelationRow `parameter:",kind=output,in=view"`
}

func TestCompileArtifactsUsesTaggedReaderRelations(t *testing.T) {
	on := true
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Regions"}, Name: "Regions",
		Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, Routes: []*spec.Route{{Method: "GET", Path: "/regions"}},
		RootView: &spec.View{Name: "regions", Groupable: &on, Source: &spec.ViewSource{SQL: "SELECT v.country, v.region, SUM(v.amount) AS amount FROM spend v GROUP BY v.country, v.region"}},
	}
	compilation, err := NewProjectCompiler(ProjectConfig{}).CompileArtifacts([]bootstrap.ArtifactInput{{
		Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[compiledRelationOutput](), DirectViewField: "Rows",
	}})
	require.NoError(t, err)
	require.Empty(t, component.RootView.Relations)
	for _, artifact := range compilation.Artifacts() {
		if !artifact.IsReport() {
			continue
		}
		plan := artifact.predefinedHandler.(*Handler).plan
		for _, tc := range []struct {
			name       string
			dimensions []string
			want       []string
		}{
			{"measure", nil, []string{"amount"}},
			{"composite", []string{"Country", "Region"}, []string{"country", "region", "amount", "Lookup"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				input := reflect.New(artifact.InputType()).Elem()
				for _, name := range tc.dimensions {
					input.FieldByName("Dimensions").FieldByName(name).SetBool(true)
				}
				input.FieldByName("Measures").FieldByName("Amount").SetBool(true)
				fields, err := plan.selectedFields(input)
				require.NoError(t, err)
				require.ElementsMatch(t, tc.want, fields)
			})
		}
		return
	}
	t.Fatal("no derived report")
}
