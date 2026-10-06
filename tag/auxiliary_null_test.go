package tag

import "testing"

func TestAuxiliaryNullTagEnumAndDuplicates(t *testing.T) {
	for _, key := range []string{"rootNullPolicy", "nestedNullPolicy"} {
		for _, value := range []string{"initial-validation", "skip-auxiliary"} {
			if _, err := ParseView("Rows,auxiliary=true," + key + "=" + value); err != nil {
				t.Fatal(err)
			}
		}
		for _, value := range []string{"skip", "true", "", "skip-auxiliary," + key + "=initial-validation"} {
			if _, err := ParseView("Rows,auxiliary=true," + key + "=" + value); err == nil {
				t.Fatalf("accepted %s=%s", key, value)
			}
		}
	}
}

func TestAuxiliaryNullTagValueRoundTrip(t *testing.T) {
	for _, root := range []bool{true, false} {
		v := View{Name: "Rows", Auxiliary: true}
		if root {
			v.RootNullPolicy = "skip-auxiliary"
		} else {
			v.NestedNullPolicy = "skip-auxiliary"
		}
		text, err := v.Value()
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseView(text)
		if err != nil || got.RootNullPolicy != v.RootNullPolicy || got.NestedNullPolicy != v.NestedNullPolicy || !got.Auxiliary {
			t.Fatalf("lost metadata: %s / %+v / %v", text, got, err)
		}
	}
}
