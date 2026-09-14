package spec

import "testing"

func TestSelectorCloneDetachesSQLMethods(t *testing.T) {
	original := &Selector{SQLMethods: []SQLMethod{{Name: "lower", Args: []string{"string"}}}}
	cloned := original.Clone()
	cloned.SQLMethods[0].Args[0] = "int"
	cloned.SQLMethods[0].Name = "abs"
	if original.SQLMethods[0].Args[0] != "string" || original.SQLMethods[0].Name != "lower" {
		t.Fatal("selector clone aliases method metadata")
	}
}
