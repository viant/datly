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

func TestReaderResolvedResourcesPreserveSourceOwnership(t *testing.T) {
	const locks = `SELECT id FROM records
WHERE $criteria.In("id", $Ids)
ORDER BY id
#if($Ids) ${View.ForUpdate()} #end`
	const dependency = `SELECT dep.id FROM (dependencies) dep`
	resources := fstest.MapFS{
		"sql/locks.sql":      {Data: []byte(locks)},
		"sql/dependency.sql": {Data: []byte(dependency)},
	}
	for _, embedded := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "embedded"}[embedded], func(t *testing.T) {
			lockSQL, dependencySQL := locks, dependency
			if embedded {
				lockSQL, dependencySQL = "${embed:sql/locks.sql}", "${embed:sql/dependency.sql}"
			}
			for _, tc := range []struct{ name, authored, analysis string }{
				{"Locks", lockSQL, locks},
				{"Dependency", `SELECT r.*,d.* FROM records r LEFT JOIN (` + dependencySQL + `) d ON d.id=r.id`, ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					input := &spec.View{Name: tc.name, Source: &spec.ViewSource{SQL: tc.authored, Embeds: dql.EmbeddedSQLRefs(tc.authored)}}
					before := input.Clone()
					analysis := tc.analysis
					if analysis == "" {
						analysis = tc.authored
					}
					got, err := NewReader().Compile(ReadInput{View: input, SQL: analysis, Resources: resources})
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, input) {
						t.Fatal("authored view mutated")
					}
					if err := dsql.ResolveSource(got.Name, got.Source, resources); err != nil {
						t.Fatal(err)
					}
					if tc.name == "Locks" {
						if n := strings.Count(got.Source.SQL, "${View.ForUpdate()}"); n != 1 {
							t.Fatalf("lock count %d: %s", n, got.Source.SQL)
						}
					} else {
						child := got.Relations[0].View
						if !child.Auxiliary || child.Source.Table != "dependencies" {
							t.Fatalf("lost dependency ownership: %+v", child)
						}
						if strings.Contains(child.Source.SQL, "(dependencies)") {
							t.Fatalf("unlowered marker: %s", child.Source.SQL)
						}
					}
					views := []*spec.View{got}
					for _, rel := range got.Relations {
						views = append(views, rel.View)
					}
					for _, view := range views {
						if strings.Contains(view.Source.SQL, "${embed:") || len(view.Source.Embeds) != 0 {
							t.Fatalf("inconsistent resolved source: %+v", view.Source)
						}
					}
				})
			}
		})
	}
}

func TestReaderEmbeddedFragmentsBelongToEachSource(t *testing.T) {
	resources := fstest.MapFS{
		"first.sql":  {Data: []byte("SELECT id FROM records\n#if($First) ${View.ForUpdate()} #end")},
		"second.sql": {Data: []byte("SELECT id FROM records\n#if($Second) ${View.ForUpdate()} #end")},
	}
	sql := `SELECT a.id AS First,b.id AS Second FROM (${embed:first.sql}) a JOIN (${embed:second.sql}) b ON b.id=a.id`
	analysis := &spec.ViewSource{SQL: sql, Embeds: dql.EmbeddedSQLRefs(sql)}
	if err := dsql.ResolveSource("Rows", analysis, resources); err != nil {
		t.Fatal(err)
	}
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql, Embeds: dql.EmbeddedSQLRefs(sql)}}, SQL: analysis.SQL, Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	for i, view := range []*spec.View{got, got.Relations[0].View} {
		if err := dsql.ResolveSource(view.Name, view.Source, resources); err != nil {
			t.Fatal(err)
		}
		want, other := "$First", "$Second"
		if i == 1 {
			want, other = other, want
		}
		if strings.Count(view.Source.SQL, "${View.ForUpdate()}") != 1 || !strings.Contains(view.Source.SQL, want) || strings.Contains(view.Source.SQL, other) {
			t.Fatalf("wrong fragment owner: %s", view.Source.SQL)
		}
	}
}

func TestReaderKeepsIdenticalFragmentsInDifferentSources(t *testing.T) {
	const fragment = "#if($Locked) ${View.ForUpdate()} #end"
	const body = "SELECT id FROM records\n" + fragment
	resources := fstest.MapFS{"locks.sql": {Data: []byte(body)}}
	const sql = `SELECT a.id AS First,b.id AS Second FROM (${embed:locks.sql}) a JOIN (${embed:locks.sql}) b ON b.id=a.id`
	analysis := &spec.ViewSource{SQL: sql, Embeds: dql.EmbeddedSQLRefs(sql)}
	if err := dsql.ResolveSource("Rows", analysis, resources); err != nil {
		t.Fatal(err)
	}
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: sql, Embeds: dql.EmbeddedSQLRefs(sql)}}, SQL: analysis.SQL, Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	for _, view := range []*spec.View{got, got.Relations[0].View} {
		if err := dsql.ResolveSource(view.Name, view.Source, resources); err != nil {
			t.Fatal(err)
		}
		if strings.Count(view.Source.SQL, fragment) != 1 {
			t.Fatalf("lost or duplicated owning fragment: %s", view.Source.SQL)
		}
	}
}

func TestReaderEmbeddedAuxiliaryTemplateKeepsLoweredSource(t *testing.T) {
	const body = "SELECT id FROM (dependencies)\nWHERE 1=1\n#if($Scoped) AND id=$ID #end\nORDER BY id\n#if($Locked) ${View.ForUpdate()} #end"
	resources := fstest.MapFS{"dependency.sql": {Data: []byte(body)}}
	const authored = "${embed:dependency.sql}"
	got, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Dependency", Source: &spec.ViewSource{SQL: authored, Embeds: dql.EmbeddedSQLRefs(authored)}}, SQL: authored, Resources: resources})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Auxiliary || strings.Contains(got.Source.SQL, "(dependencies)") {
		t.Fatalf("restored auxiliary marker: %s", got.Source.SQL)
	}
	for _, fragment := range []string{"#if($Scoped) AND id=$ID #end", "#if($Locked) ${View.ForUpdate()} #end"} {
		if strings.Count(got.Source.SQL, fragment) != 1 {
			t.Fatalf("lost or duplicated template: %s", got.Source.SQL)
		}
	}
}
