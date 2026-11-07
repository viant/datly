package sqlanalysis

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestConditionalSQLAlternatives(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		want         []string
	}{
		{"filter", "SELECT id FROM items WHERE 1=1 #if($ID) AND id=$ID #end", []string{"SELECT id FROM items WHERE 1=1  AND id=$ID ", "SELECT id FROM items WHERE 1=1 "}},
		{"elseif", "SELECT #if($A) a #elseif($B) b #else c #end FROM items", []string{"SELECT  a  FROM items", "SELECT  b  FROM items", "SELECT  c  FROM items"}},
		{"else", "SELECT #if($Flag) id #else name #end FROM items", []string{"SELECT  id  FROM items", "SELECT  name  FROM items"}},
		{"nested", "SELECT id FROM items #if($A) WHERE a=1 #if($B) AND b=2 #end#else WHERE a=2 #end", []string{"SELECT id FROM items  WHERE a=1  AND b=2 ", "SELECT id FROM items  WHERE a=1 ", "SELECT id FROM items  WHERE a=2 "}},
		{"protected with branch", "SELECT '#if($X)' AS label FROM items #if($ID) WHERE id=$ID #end", []string{"SELECT '#if($X)' AS label FROM items  WHERE id=$ID ", "SELECT '#if($X)' AS label FROM items "}},
		{"quoted", "SELECT '#if($Flag)' AS label /* #end */ FROM items", []string{"SELECT '#if($Flag)' AS label /* #end */ FROM items"}},
		{"opaque condition", "SELECT id FROM items #if($Input.NeverExecute()) WHERE id=1 #end", []string{"SELECT id FROM items  WHERE id=1 ", "SELECT id FROM items "}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConditionalSQL(tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestConditionalSQLIndexedComponentBinding(t *testing.T) {
	source := `#set($SelectedId = $Dependency.Data[0].Id)
SELECT id FROM records WHERE id = $SelectedId`
	variants, err := ConditionalSQL(source)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"\nSELECT id FROM records WHERE id = ${Dependency.Data[0].Id}"}
	if !reflect.DeepEqual(variants, want) {
		t.Fatalf("got %#v, want %#v", variants, want)
	}
}

func TestConditionalSQLRejectsEvaluatedIndexBindings(t *testing.T) {
	for _, selector := range []string{"$Dependency.Data[$Index].Id", "$Dependency.Data[1.5].Id", "$Dependency.Data[0].Lookup()"} {
		source := "#set($SelectedId = " + selector + ") SELECT id FROM records WHERE id=$SelectedId"
		if _, err := ConditionalSQL(source); err == nil {
			t.Fatalf("accepted evaluated selector %s", selector)
		}
	}
}
func TestConditionalSQLRejectsMalformedAndUnbounded(t *testing.T) {
	for _, source := range []string{"SELECT id #if($A)", "SELECT id #end", "SELECT id #else", "SELECT id #elseif($A)", "SELECT id #if($A WHERE id=1 #end", distinctConditionalColumns(7)} {
		if _, err := ConditionalSQL(source); err == nil {
			t.Fatalf("accepted %q", source)
		}
	}
}

func distinctConditionalColumns(count int) string {
	var source strings.Builder
	source.WriteString("SELECT id")
	for i := 0; i < count; i++ {
		fmt.Fprintf(&source, " #if($A) ,col%d #end", i)
	}
	return source.String()
}
func TestConditionalSQLCollapsesEquivalentAlternatives(t *testing.T) {
	source := "SELECT id" + strings.Repeat("#if($A)#end", 12) + " FROM items"
	variants, err := ConditionalSQL(source)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(variants, []string{"SELECT id FROM items"}) {
		t.Fatalf("equivalent branches: %#v", variants)
	}
}
func TestConditionalSQLConstantConditionsAndProtectedMarkers(t *testing.T) {
	source := `SELECT '__datly_conditional_literal_dollar #end' AS label FROM items #if(false) WHERE bad=1 #elseif(true) WHERE id=1 #else WHERE bad=2 #end`
	variants, err := ConditionalSQL(source)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`SELECT '__datly_conditional_literal_dollar #end' AS label FROM items  WHERE id=1 `}
	if !reflect.DeepEqual(variants, want) {
		t.Fatalf("got %#v, want %#v", variants, want)
	}
}
