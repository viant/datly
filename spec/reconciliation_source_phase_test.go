package spec

import (
	"strings"
	"testing"
)

func TestSourcePhaseDescriptorRemainsUnavailable(t *testing.T) {
	config, err := ParseReconciliation(`{"mode":"source-phases","rootAction":"all-supplied-positive-keys","roles":[{"holder":"Children"}]}`)
	if err != nil {
		t.Fatal(err)
	}
	clone := config.Clone()
	if clone.RootAction != config.RootAction {
		t.Fatal("root decision lost on clone")
	}
	err = ValidateReconciliationView(&View{Reconciliation: clone}, "patch")
	if err == nil || !strings.Contains(err.Error(), "unavailable until phase/allocation/payload") {
		t.Fatal(err)
	}
	for _, text := range []string{
		`{"mode":"source-phases","roles":[{"holder":"Children"}]}`,
		`{"mode":"source-phases","rootAction":"unknown","roles":[{"holder":"Children"}]}`,
		`{"mode":"same-parent-root-first","rootAction":"all-supplied-positive-keys","roles":[{"holder":"Children"}]}`,
	} {
		if _, err = ParseReconciliation(text); err == nil {
			t.Fatal("invalid root decision accepted", text)
		}
	}
	if _, err = ParseReconciliation(`{"mode":"same-parent-root-first","roles":[{"holder":"Children"}]}`); err != nil {
		t.Fatal("existing mode regressed", err)
	}
}
