package tag

import "testing"

func TestRootNullPolicyRoundtrip(t *testing.T) {
	text, err := (View{Name: "Rows", RootNullPolicy: "initial-validation"}).Value()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseView(text)
	if err != nil || parsed.RootNullPolicy != "initial-validation" {
		t.Fatal(text, err)
	}
	for _, text := range []string{"Rows,rootNullPolicy=bad", "Rows,rootNullPolicy=initial-validation,rootNullPolicy=initial-validation"} {
		if _, err = ParseView(text); err == nil {
			t.Fatal("invalid tag accepted")
		}
	}
	if _, err = (View{RootNullPolicy: "invalid"}).Value(); err == nil {
		t.Fatal("invalid policy emitted")
	}
}
