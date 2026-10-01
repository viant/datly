package tag

import (
	"strings"
	"testing"
)

func TestInsertValidationPresenceRoundtrip(t *testing.T) {
	v, err := (View{Name: "Rows", InsertValidationPresence: true}).Value()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseView(v)
	if err != nil || !parsed.InsertValidationPresence {
		t.Fatalf("%s %+v %v", v, parsed, err)
	}
	defaultTag, err := (View{Name: "Rows"}).Value()
	if err != nil || strings.Contains(defaultTag, "insertValidationPresence") {
		t.Fatalf("default changed %s %v", defaultTag, err)
	}
	if _, err := ParseView("Rows,insertValidationPresence=wrong"); err == nil {
		t.Fatal("invalid bool accepted")
	}
}
