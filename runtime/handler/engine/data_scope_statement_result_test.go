package engine

import (
	"reflect"
	"testing"
)

type resultDMLProbe struct {
	matchedDMLProbe
	statement string
	dest      any
	args      []any
	calls     int
}

func (p *resultDMLProbe) ExecuteWithResult(statement string, dest any, args ...any) error {
	p.calls++
	p.statement, p.dest, p.args = statement, dest, append([]any(nil), args...)
	return nil
}

func TestFocusedDMLStatementResultCapability(t *testing.T) {
	var id int64
	probe := &resultDMLProbe{}
	capability := dmlCapability{service: probe}
	if err := capability.ExecuteWithResult("INSERT INTO records(name) VALUES (?)", &id, "name"); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 1 || probe.dest != &id || probe.statement != "INSERT INTO records(name) VALUES (?)" || !reflect.DeepEqual(probe.args, []any{"name"}) {
		t.Fatalf("result capability changed arguments: %+v", probe)
	}
	for _, guard := range []*mutationGuard{{depth: 1}, {completionClosed: true}, {guardedFailure: ErrWriteEligibilityMutation}} {
		capability.guard = guard
		if err := capability.ExecuteWithResult("unused", &id); err == nil {
			t.Fatal("guard admitted result statement")
		}
		if probe.calls != 1 || id != 0 {
			t.Fatal("rejected capability executed or mutated destination")
		}
	}
	unsupported := dmlCapability{service: &matchedDMLProbe{}}
	if err := unsupported.ExecuteWithResult("unused", &id); err == nil {
		t.Fatal("missing optional capability silently accepted")
	}
}
