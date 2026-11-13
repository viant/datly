package sqlanalysis

import (
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/query"
	"reflect"
	"strings"
	"testing"
)

func TestConditionalBoundValuePrelude(t *testing.T) {
	source := `#set($selected = -1)
#if($Unsafe.Has.Ids)
#set($selected = $Unsafe.Ids[0])
#end
SELECT r.id, w.weight FROM records r LEFT JOIN record_weights w ON w.record_id=r.id AND w.owner_id=$criteria.AppendBinding($selected)`
	variants, err := ConditionalSQL(source)
	if err != nil {
		t.Fatal(err)
	}
	if len(variants) == 0 {
		t.Fatal("no analysis alternatives")
	}
	for _, variant := range variants {
		analysis, err := SQL(variant, nil)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(analysis, "$Unsafe") || strings.Contains(analysis, "#set") {
			t.Fatalf("template leaked into analysis: %s", analysis)
		}
	}
}

func TestPreparedSQLKeepsBindingValuesOpaque(t *testing.T) {
	for _, source := range []string{
		`#set($selected = -1) SELECT id FROM records WHERE id=$criteria.AppendBinding($selected)`,
		`(#set($selected = -1) #if($Unsafe.Has.Ids) #set($selected=$Unsafe.Ids[0]) #end SELECT id FROM records WHERE id=$criteria.AppendBinding($selected))`,
		`#if($Unsafe.NeverExecute()) #set($selected=1) #else #set($selected=2) #end SELECT id FROM records WHERE id=$criteria.AppendBinding($selected)`,
		`#set($selected=$Dependency.Id) #set($selected=-1) SELECT id FROM records WHERE id=$criteria.AppendBinding($selected)`,
	} {
		if _, err := PreparedSQL(source, nil); err != nil {
			t.Fatalf("bound value rejected: %v", err)
		}
	}
}

func TestPreparedSQLRejectsChangingOrUnprovenIdentity(t *testing.T) {
	for _, source := range []string{
		`SELECT $Unsafe.Columns FROM records`,
		`#set($criteria=$Other) SELECT id FROM records WHERE id=$criteria.AppendBinding(1)`,
		`SELECT id FROM $Unsafe.Table`,
		`#set($selected=-1) SELECT $selected FROM records`,
		`#set($selected=$Dependency.Id) #set($selected=-1) SELECT $selected FROM records`,
		`#if($Flag) SELECT id FROM records #else SELECT name FROM records #end`,
		`#if($Flag) SELECT a AS value FROM records #else SELECT b AS value FROM records #end`,
		`#if($Flag) SELECT id FROM records #else SELECT id FROM other_records #end`,
		`#if($Flag) SELECT id FROM records LIMIT 1 #else SELECT id FROM records LIMIT 2 #end`,
		`#if($Flag) SELECT id FROM records LIMIT 1 OFFSET 0 #else SELECT id FROM records LIMIT 1 OFFSET 1 #end`,
		`#set($selected=-1) SELECT id FROM records WHERE id=$criteria.AppendBinding()`,
		`#set($selected=-1) SELECT id FROM records WHERE id=$criteria.AppendBinding($selected,1)`,
		`#set($selected=-1) SELECT id FROM records WHERE id=$criteria.AppendBinding($selected).SQL`,
		`#set($selected=-1) SELECT id FROM $criteria.AppendBinding($selected)`,
	} {
		if _, err := PreparedSQL(source, nil); err == nil {
			t.Fatalf("accepted changing/unproven SQL: %s", source)
		}
	}
}

func TestPreparedSQLNestedBoundPrelude(t *testing.T) {
	source := `SELECT nested.* FROM (#set($selected=-1)
#if($Unsafe.Has.Ids)
#set($selected=$Unsafe.Ids[0])
#end
SELECT id FROM records WHERE id=$criteria.AppendBinding($selected)) nested`
	_, err := PreparedSQL(source, nil)
	if err != nil {
		variants, _ := ConditionalSQL(source)
		for _, variant := range variants {
			analysis, _ := SQL(variant, nil)
			t.Logf("analysis: %q", analysis)
		}
		t.Fatal(err)
	}
}

func TestPreparedSQLRejectsDistinctCheckedASTs(t *testing.T) {
	for _, source := range []string{
		`#if($Flag) SELECT r.id FROM records r JOIN weights w ON w.id=r.id #else SELECT r.id FROM records r JOIN weights w ON w.other_id=r.id #end`,
		`#if($Flag) SELECT a+1 AS value FROM records #else SELECT a+2 AS value FROM records #end`,
		`#if($Flag) WITH source AS (SELECT id FROM records) SELECT id FROM source #else WITH source AS (SELECT id FROM other_records) SELECT id FROM source #end`,
		`#if($Flag) SELECT id FROM records LIMIT 1 #else SELECT id FROM records LIMIT 2 #end`,
		`#if($Flag) SELECT id FROM records LIMIT 1 OFFSET 0 #else SELECT id FROM records LIMIT 1 OFFSET 1 #end`,
	} {
		_, err := PreparedSQL(source, nil)
		if err == nil || !strings.Contains(err.Error(), "changes its analyzed statement") {
			t.Fatalf("expected checked AST identity rejection, got %v for %s", err, source)
		}
	}
}

func TestPreparedIdentityPreservesResidualRawContent(t *testing.T) {
	for _, parsed := range []bool{false, true} {
		makeQuery := func(raw, remainder string) *query.Select {
			source := &expr.Raw{Raw: raw, Unparsed: remainder}
			if parsed {
				source.X = &query.Select{List: query.List{query.NewItem(&expr.Ident{Name: "id"})}}
			}
			return &query.Select{From: query.From{X: source}}
		}
		a := preparedIdentity(makeQuery("(SELECT id)", "tail one"))
		b := preparedIdentity(makeQuery("(SELECT id)", "tail two"))
		if reflect.DeepEqual(a, b) {
			t.Fatal("residual source text was discarded")
		}
		a = preparedIdentity(makeQuery("(SELECT id)", ""))
		b = preparedIdentity(makeQuery("( SELECT id )", ""))
		if reflect.DeepEqual(a, b) != parsed {
			t.Fatal("only checked subquery text may be normalized")
		}
	}
}
