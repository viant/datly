package tag

import "testing"

func TestNestedNullPolicyRoundtrip(t *testing.T) {
	value, err := (View{Name: "Children", NestedNullPolicy: "initial-validation"}).Value()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseView(value)
	if err != nil || parsed.NestedNullPolicy != "initial-validation" || parsed.RootNullPolicy != "" {
		t.Fatal(value, err)
	}
	for _, text := range []string{"Children,nestedNullPolicy=bad", "Children,nestedNullPolicy=initial-validation,nestedNullPolicy=initial-validation"} {
		if _, err := ParseView(text); err == nil {
			t.Fatal("invalid tag accepted")
		}
	}
	if _, err := (View{NestedNullPolicy: "bad"}).Value(); err == nil {
		t.Fatal("invalid policy emitted")
	}
}
