package tag

import "testing"

func TestAuxiliaryViewTagRoundTrip(t *testing.T) {
	for _, auxiliary := range []bool{false, true} {
		value, err := (View{Name: "Lookup", Table: "LOOKUP", Auxiliary: auxiliary}).Value()
		if err != nil {
			t.Fatal(err)
		}
		actual, err := ParseView(value)
		if err != nil || actual.Auxiliary != auxiliary || actual.Table != "LOOKUP" {
			t.Fatalf("value=%s actual=%+v error=%v", value, actual, err)
		}
	}
	if _, err := ParseView("Lookup,auxiliary=maybe"); err == nil {
		t.Fatal("invalid auxiliary intent accepted")
	}
}
