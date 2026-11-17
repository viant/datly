package compile

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestReaderPreservesConditionalQueryBranches(t *testing.T) {
	for _, prefix := range []string{" ", "\n"} {
		name := "inline"
		if prefix == "\n" {
			name = "line_start"
		}
		t.Run(name, func(t *testing.T) {
			body := "#if($Enabled)\nSELECT a.id FROM active_records a\n#else\nSELECT b.id FROM archived_records b\n#end"
			sql := "SELECT r.*,use_connector(r,'main') FROM (" + prefix + body + ") r"
			actual, err := NewReader().Compile(ReadInput{SQL: sql, View: &spec.View{Name: "reader", Source: &spec.ViewSource{SQL: sql}}})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(actual.Source.SQL, body) {
				t.Fatalf("conditional query was changed: %s", actual.Source.SQL)
			}
			if actual.Source.Table != "" {
				t.Fatalf("one branch incorrectly established physical table authority: %s", actual.Source.Table)
			}
		})
	}
}

func TestReaderConditionalQueriesKeepEveryBranchValidation(t *testing.T) {
	body := "#if($Enabled) SELECT a.id FROM active_records a #else SELECT b.id,tag(b.id,'json:\"hidden\"') FROM archived_records b #end"
	sql := "SELECT r.*,use_connector(r,'main') FROM (" + body + ") r"
	_, err := NewReader().Compile(ReadInput{SQL: sql, View: &spec.View{Name: "reader", Source: &spec.ViewSource{SQL: sql}}})
	if err == nil || !strings.Contains(err.Error(), CodeViewDirective) {
		t.Fatalf("alternate branch hid nested metadata: %v", err)
	}
}

func TestReaderConditionalQueriesWithCTEsAndPredicates(t *testing.T) {
	body := `#if($Enabled)
WITH RECURSIVE levels AS (
 SELECT a.id FROM active_records a
 ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}
 AND a.enabled=1
 UNION ALL SELECT c.id FROM active_records c JOIN levels l ON c.parent_id=l.id
)
SELECT id FROM levels
#else
SELECT b.id FROM archived_records b
WHERE 1=1 ${predicate.Builder().CombineAnd($predicate.FilterGroup(1,"AND")).Build("AND")}
#end`
	sql := "SELECT r.*,use_connector(r,'main') FROM ((" + body + ")) r"
	actual, err := NewReader().Compile(ReadInput{SQL: sql, View: &spec.View{Name: "reader", Source: &spec.ViewSource{SQL: sql}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(actual.Source.SQL, body) {
		t.Fatalf("conditional CTE/predicate query was changed: %s", actual.Source.SQL)
	}
}

func TestReadTemplateScannerIgnoresQuotedAndCommentedDirectives(t *testing.T) {
	for _, source := range []string{
		`SELECT '#if($Enabled)', "#else", ` + "`#end`" + ` FROM records`,
		"SELECT id FROM records -- #if($Enabled)\nWHERE 1=1 /* #else #end */",
	} {
		actual, err := newReadTemplateSource(source)
		if err != nil {
			t.Fatal(err)
		}
		if actual.AnalysisSQL != source || len(actual.fragments) != 0 {
			t.Fatalf("quoted/commented directives were interpreted: %#v", actual)
		}
	}
}
