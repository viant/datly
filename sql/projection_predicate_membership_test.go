package sql

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"strings"
	"testing"
)

const membershipPredicate = `${predicate.Builder().CombineOr($predicate.FilterGroup(2, "AND")).Build("OR")}`

func membershipCASE() string {
	return `CASE WHEN 1 = 0 ` + membershipPredicate + ` THEN m.occurred_at ELSE DATETIME_TRUNC(m.occurred_at, DAY) END`
}
func TestPredicateCaseMembershipRetainsAuthoredIdentity(t *testing.T) {
	source := `SELECT ` + membershipCASE() + ` AS bucket, m.record_id AS record_id FROM records m`
	p := SelectorProjection{SQL: source}
	for _, name := range []string{"bucket", "record_id"} {
		found, complete, err := p.HasOutput(name)
		if err != nil || !found || !complete {
			t.Fatalf("%s: %v/%v %v", name, found, complete, err)
		}
	}
	for _, name := range []string{"missing", "occurred_at", "FilterGroup", "__datly_analysis_predicate_0"} {
		found, complete, err := p.HasOutput(name)
		if err != nil || found || !complete {
			t.Fatalf("unowned %s: %v/%v %v", name, found, complete, err)
		}
		if _, err = p.Columns([]string{name}); err == nil {
			t.Fatalf("unowned selection %s admitted", name)
		}
	}
	columns, err := p.Columns([]string{"bucket", "record_id"})
	if err != nil || len(columns) != 2 {
		t.Fatalf("columns=%v err=%v", columns, err)
	}
	if columns[0].OrderExpression() != "bucket" || columns[1].OrderExpression() != "record_id" {
		t.Fatal("order lost output aliases")
	}
	if columns[0].SourceExpression() != membershipCASE() {
		t.Fatalf("authored expression changed: %s", columns[0].SourceExpression())
	}
	out, err := p.Prepare(nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Source != source {
		t.Fatalf("source changed: %s", out.Source)
	}
	for _, sql := range []string{out.Source, columns[0].SourceExpression(), columns[0].OrderExpression()} {
		if strings.Contains(sql, "__datly_analysis_predicate_") {
			t.Fatal("analysis marker escaped")
		}
	}
	// The existing executable parser has no membership-only template authority.
	parts, ok := newSelectProjectionSource(source)
	if !ok {
		t.Fatal("parts missing")
	}
	if _, err := parts.parse(); err == nil {
		t.Fatal("executable parser admitted analysis AST")
	}
}
func TestPredicateCaseMembershipRejectsUnsupportedStructure(t *testing.T) {
	cases := map[string]string{
		"unaliased":       membershipCASE(),
		"unknown builder": strings.Replace(membershipCASE(), "CombineOr", "Unknown", 1) + " AS bucket",
		"unsafe":          `CASE WHEN 1=0 $Unsafe.Filter THEN 1 ELSE 0 END AS bucket`,
		"input context":   `CASE WHEN 1=0 ` + membershipPredicate + ` THEN ${Input.Enabled} ELSE 0 END AS bucket`,
		"malformed":       `CASE WHEN 1=0 ` + membershipPredicate + ` THEN 1 ELSE END AS bucket`,
		"then":            `CASE WHEN 1=0 ` + membershipPredicate + ` THEN ` + membershipPredicate + ` ELSE 0 END AS bucket`,
		"else":            `CASE WHEN 1=0 ` + membershipPredicate + ` THEN 1 ELSE ` + membershipPredicate + ` END AS bucket`,
		"simple selector": `CASE WHEN 1=0 ` + membershipPredicate + ` THEN CASE ` + membershipPredicate + ` WHEN 1 THEN 1 ELSE 0 END ELSE 0 END AS bucket`,
		"simple value":    `CASE WHEN 1=0 ` + membershipPredicate + ` THEN CASE m.kind WHEN ` + membershipPredicate + ` THEN 1 ELSE 0 END ELSE 0 END AS bucket`,
		"build keyword":   strings.Replace(membershipCASE(), `Build("OR")`, `Build("WHERE")`, 1) + " AS bucket",
		"duplicate":       membershipCASE() + ` AS bucket, m.record_id AS bucket`,
	}
	for name, item := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := (SelectorProjection{SQL: "SELECT " + item + " FROM records m"}).HasOutput("bucket")
			if err == nil {
				t.Fatal("unsupported membership admitted")
			}
		})
	}
}
func TestPredicateMembershipCannotFeedExecutableGroupedRewrite(t *testing.T) {
	source := `SELECT ` + membershipCASE() + ` AS bucket, SUM(m.amount) AS total FROM records m`
	groupable := true
	view := &data.View{Spec: spec.View{Groupable: &groupable}}
	p := SelectorProjection{SQL: source, View: view}
	if _, err := p.Columns([]string{"bucket", "total"}); err != nil {
		t.Fatal(err)
	}
	for _, fields := range [][]string{{"bucket", "total"}, {"total"}, {"bucket"}} {
		out, err := p.Prepare(fields)
		if out != nil && (strings.Contains(out.Source, "__datly_analysis_predicate_") || strings.Contains(strings.Join(out.outer, ","), "__datly_analysis_predicate_")) {
			t.Fatal("analysis marker leaked to grouped SQL")
		}
		if err == nil {
			t.Fatalf("raw template executable rewrite unexpectedly admitted fields %v", fields)
		}
	}
}
