package compile

import (
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/dql"
)

func TestReaderRewriteKeepsEmbedMetadataConsistent(t *testing.T) {
	const authored = `SELECT r.* FROM (${embed:records.sql}) r`
	resources := fstest.MapFS{"records.sql": {Data: []byte("SELECT id,name FROM records")}}
	for _, tc := range []struct {
		name, sql    string
		materialized bool
	}{
		{"cast", `SELECT r.*,CAST(r.id AS int) FROM records r`, true},
		{"nullability", `SELECT r.*,allow_nulls(r) FROM records r`, true},
		{"selector", `SELECT r.*,set_limit(r,10) FROM records r`, true},
		{"exclusion", `SELECT r.* EXCEPT name FROM records r`, true},
		{"retained embed", `SELECT r.*,CAST(r.id AS int) FROM (${embed:records.sql}) r`, false},
		{"no rewrite", `SELECT r.* FROM records r`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: authored, Embeds: dql.EmbeddedSQLRefs(authored)}}
			before := input.Source.Clone()
			got, err := NewReader().Compile(ReadInput{View: input, SQL: tc.sql})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, input.Source) {
				t.Fatal("compiler mutated authored source")
			}
			if !reflect.DeepEqual(got.Source.Embeds, dql.EmbeddedSQLRefs(got.Source.SQL)) {
				t.Fatalf("stale references SQL=%s refs=%+v", got.Source.SQL, got.Source.Embeds)
			}
			if tc.materialized && len(got.Source.Embeds) != 0 {
				t.Fatal("consumed reference retained")
			}
			if !tc.materialized && len(got.Source.Embeds) != 1 {
				t.Fatal("real source reference lost")
			}
			if err = dsql.ResolveSource("Rows", got.Source, resources); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(got.Source.SQL, "${embed:") {
				t.Fatal("unresolved source")
			}
		})
	}
	// A malformed unchanged source remains rejected; reconciliation belongs only
	// to a compiler-owned SQL replacement, never to the strict resolver.
	invalid := &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT r.* FROM records r", Embeds: dql.EmbeddedSQLRefs(authored)}}
	got, err := NewReader().Compile(ReadInput{View: invalid, SQL: invalid.Source.SQL})
	if err != nil {
		t.Fatal(err)
	}
	if err = dsql.ResolveSource("Rows", got.Source, resources); err == nil || !strings.Contains(err.Error(), "does not contain resource token") {
		t.Fatalf("malformed unchanged metadata accepted: %v", err)
	}
}
