package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestCompiledSQLXAliasMetadataRetainsClaimsAndNativeFirstMatch(t *testing.T) {
	type row struct {
		ID      int    `sqlx:"id"`
		Primary string `sqlx:"physical_id|shared_result|primary_fallback" json:"primary"`
		Other   string `sqlx:"other_id|shared_result|other_fallback" json:"other"`
	}
	type output struct {
		Data []*row `json:"data"`
	}

	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{
			Name: "ProjectionAliases",
			RootView: &spec.View{
				Name:   "records",
				Source: &spec.ViewSource{SQL: "SELECT id, physical_id, other_id FROM records"},
			},
		},
		InputType:       reflect.TypeFor[struct{}](),
		OutputType:      reflect.TypeFor[output](),
		DirectViewField: "Data",
	})
	if err != nil {
		t.Fatalf("BuildArtifact: %v", err)
	}
	if artifact.Reader == nil || artifact.Reader.Root == nil || artifact.Reader.Root.View == nil {
		t.Fatal("compiled projection is missing its root view")
	}
	fields := artifact.Reader.Root.View.SelectorFields
	byName := make(map[string]struct {
		column   string
		aliases  []string
		verified []string
	}, len(fields))
	for _, field := range fields {
		if field.Holder {
			continue
		}
		byName[field.GoName] = struct {
			column   string
			aliases  []string
			verified []string
		}{field.Column, field.ColumnAliases, field.VerifiedColumnAliases}
	}

	primary, ok := byName["Primary"]
	if !ok {
		t.Fatalf("compiled primary field missing: %#v", fields)
	}
	if primary.column != "physical_id" || !reflect.DeepEqual(primary.aliases, []string{"shared_result", "primary_fallback"}) {
		t.Fatalf("primary mapping = column %q, aliases %#v", primary.column, primary.aliases)
	}
	if !reflect.DeepEqual(primary.verified, []string{"shared_result", "primary_fallback"}) {
		t.Fatalf("native matcher verified primary aliases = %#v", primary.verified)
	}

	other, ok := byName["Other"]
	if !ok {
		t.Fatalf("compiled other field missing: %#v", fields)
	}
	if other.column != "other_id" || !reflect.DeepEqual(other.aliases, []string{"shared_result", "other_fallback"}) {
		t.Fatalf("other mapping = column %q, aliases %#v", other.column, other.aliases)
	}
	if !reflect.DeepEqual(other.verified, []string{"other_fallback"}) {
		t.Fatalf("native matcher should assign shared_result to first field; other verified aliases = %#v", other.verified)
	}
}
