package sqlanalysis

import (
	"fmt"
	"strings"
	"testing"

	veltyexpr "github.com/viant/velty/ast/expr"
)

func TestParseExistingCriteriaInKeepsNamedRowIdentity(t *testing.T) {
	source := `SELECT pcle.* FROM CI_POSTAL_CODE_LIST_ENTRY pcle JOIN CI_POSTAL_CODE_LIST l ON l.ID = pcle.LIST_ID WHERE $criteria.In("l.ID", $PostalCodeListIds.Values) AND l.ACTIVE = 1`
	text, parsed, err := Parse(source, nil)
	if err != nil {
		t.Fatal(err)
	}
	if parsed == nil || !strings.Contains(text, "CI_POSTAL_CODE_LIST_ENTRY") || strings.Contains(text, "$criteria.In") {
		t.Fatalf("invalid analysis: %s", text)
	}
	if !strings.Contains(source, `$criteria.In("l.ID", $PostalCodeListIds.Values)`) {
		t.Fatal("executable SQL changed")
	}
}

func TestCriteriaInRetainsDeclaredOperandValidation(t *testing.T) {
	source := `SELECT l.ID FROM records l WHERE $criteria.In("l.ID", $Ids.Values)`
	calls := 0
	_, _, err := Parse(source, func(value *veltyexpr.Select) (string, error) {
		calls++
		if value.ID != "Ids" {
			t.Fatalf("wrong operand: %s", value.ID)
		}
		return "", fmt.Errorf("undeclared input")
	})
	if calls != 1 || err == nil || !strings.Contains(err.Error(), "undeclared input") {
		t.Fatalf("validation lost: calls=%d err=%v", calls, err)
	}
}

func TestCriteriaInCannotSupplySQLIdentity(t *testing.T) {
	for _, source := range []string{
		`SELECT $criteria.In("l.ID", $Ids.Values) AS injected FROM records l`,
		`SELECT l.ID FROM $criteria.In("l.ID", $Ids.Values) l`,
		`SELECT l.ID FROM records l WHERE $criteria.In($Column, $Ids.Values)`,
		`SELECT l.ID FROM records l WHERE $criteria.In("l.ID OR 1=1", $Ids.Values)`,
		`SELECT l.ID FROM records l WHERE $criteria.In("l.ID", $Ids.Load())`,
		`SELECT l.ID FROM records l WHERE $criteria.In("l.ID", $Ids.Values).Other()`,
		`SELECT l.ID FROM records l ORDER BY $criteria.In("l.ID", $Ids.Values)`,
	} {
		t.Run(source, func(t *testing.T) {
			if _, _, err := Parse(source, nil); err == nil {
				t.Fatal("dynamic identity accepted")
			}
		})
	}
}
