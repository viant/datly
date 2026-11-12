package tag

import "testing"

func TestRemovedAuxiliaryNullPolicyRejected(t *testing.T) {
	for _, key := range []string{"rootNullPolicy", "nestedNullPolicy"} {
		if _, err := ParseView("Rows,auxiliary=true," + key + "=skip-auxiliary"); err == nil {
			t.Fatal("removed policy admitted")
		}
		v := View{Name: "Rows", Auxiliary: true}
		if key == "rootNullPolicy" {
			v.RootNullPolicy = "skip-auxiliary"
		} else {
			v.NestedNullPolicy = "skip-auxiliary"
		}
		if _, err := v.Value(); err == nil {
			t.Fatal("removed policy serialized")
		}
	}
}
