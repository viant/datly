package sqlanalysis

import (
	"fmt"
	"strings"
	"testing"

	"github.com/viant/velty/ast/expr"
)

func loopInputs(selector *expr.Select) (string, error) {
	if selector.ID != "Members" && selector.ID != "Alternates" {
		return "", fmt.Errorf("undeclared input %s", selector.ID)
	}
	return selector.ID, nil
}

func TestConditionalSQLPredicateForEach(t *testing.T) {
	loop := `#foreach($member in $Members)(f.name=$member.Name OR (f.name IS NULL AND $member.Name IS NULL))#if($foreach.HasNext) OR #end#end`
	for _, source := range []string{
		`SELECT f.name AS label FROM facts f WHERE (` + loop + `)`,
		`SELECT CASE WHEN (#if($Members)` + loop + `#else FALSE#end) THEN f.name ELSE 'Other' END AS label FROM facts f`,
		`#if($Mode)#set($selected=$Members)#else#set($selected=$Alternates)#end SELECT f.name AS label FROM facts f WHERE (#foreach($member in $selected)#set($name=$member.Name) f.name=$name #if($foreach.HasNext) OR #end#end)`,
	} {
		variants, err := ConditionalSQL(source, loopInputs)
		if err != nil {
			t.Fatal(err)
		}
		if len(variants) < 2 {
			t.Fatalf("missing loop alternatives: %#v", variants)
		}
		for _, variant := range variants {
			if strings.Contains(variant, "$member") || strings.Contains(variant, "$name") || strings.Contains(variant, "$selected") {
				t.Fatalf("unresolved local: %s", variant)
			}
		}
	}
}
func TestConditionalSQLPredicateForEachRejects(t *testing.T) {
	for _, source := range []string{
		`SELECT #foreach($member in $Members) f.name=$member.Name #end AS label FROM facts f`,
		`SELECT f.name FROM #foreach($member in $Members) facts #end f`,
		`SELECT f.name AS label FROM facts f ORDER BY #foreach($member in $Members) f.name=$member.Name #end`,
		`SELECT CASE WHEN TRUE THEN #foreach($member in $Members) f.name=$member.Name #end ELSE FALSE END AS label FROM facts f`,
		`SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Missing) f.name=$member.Name #end)`,
		`SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Members) f.name=$member.Name #end) AND f.name=$member.Name`,
		`#if($Mode)#set($selected=$Members)#end SELECT f.name AS label FROM facts f WHERE (#foreach($member in $selected) f.name=$member.Name #end)`,
		`SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Members) f.name=$member.Name #else FALSE #end)`,
		`SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Members) f.name=$member.Name)`,
		`SELECT f.name AS label FROM facts f WHERE (#foreach($member in $Members) f.name=$member.Name #if($foreach.HasNext) OR #end #end #end)`,
	} {
		if _, err := ConditionalSQL(source, loopInputs); err == nil {
			t.Fatalf("accepted %s", source)
		}
	}
}

func TestConditionalSQLPredicateLoopMarkersAreIsolated(t *testing.T) {
	var source strings.Builder
	source.WriteString("SELECT f.name AS label FROM facts f WHERE (")
	for i := 0; i < 12; i++ {
		if i == 0 {
			source.WriteString("#if($Flag)")
		} else {
			source.WriteString("#elseif($Flag)")
		}
		source.WriteString(`#foreach($member in $Members) f.name=$member.Name #if($foreach.HasNext) OR #end#end`)
	}
	source.WriteString("#else FALSE#end)")
	variants, err := ConditionalSQL(source.String(), loopInputs)
	if err != nil {
		t.Fatal(err)
	}
	if len(variants) != 25 {
		t.Fatalf("got %d branches, want 25", len(variants))
	}
}
