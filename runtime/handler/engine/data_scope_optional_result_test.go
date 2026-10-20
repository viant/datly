package engine

import (
	"context"
	"errors"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"reflect"
	"testing"
)

type optionalResultProbe struct {
	resultDMLProbe
	failure error
}

func (p *optionalResultProbe) ExecuteWithOptionalResult(statement string, dest any, args ...any) error {
	_ = p.ExecuteWithResult(statement, dest, args...)
	return p.failure
}
func TestFocusedDMLOptionalResultCapability(t *testing.T) {
	p := &optionalResultProbe{failure: errors.New("service error")}
	c := dmlCapability{service: p}
	var id int
	if err := c.ExecuteWithOptionalResult("insert", &id, 7, "x"); !errors.Is(err, p.failure) {
		t.Fatal(err)
	}
	if p.calls != 1 || p.dest != &id || p.statement != "insert" || !reflect.DeepEqual(p.args, []any{7, "x"}) {
		t.Fatal("forwarding changed")
	}
	for _, guard := range []*mutationGuard{{depth: 1}, {completionClosed: true}, {guardedFailure: ErrWriteEligibilityMutation}} {
		c.guard = guard
		if err := c.ExecuteWithOptionalResult("unused", &id); err == nil || p.calls != 1 {
			t.Fatal("guard bypassed")
		}
	}
	if err := (dmlCapability{service: &matchedDMLProbe{}}).ExecuteWithOptionalResult("unused", &id); err == nil {
		t.Fatal("unsupported capability admitted")
	}
}
func TestFocusedDMLOptionalResultSQLiteDrain(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE optional_records(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := dml.NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	row := &struct{ Id int }{}
	c := dmlCapability{service: data}
	if err := c.ExecuteWithOptionalResult("INSERT INTO optional_records(name) VALUES (?)", &row.Id, "name"); err != nil {
		t.Fatal(err)
	}
	if row.Id != 0 {
		t.Fatal("assigned before drain")
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if row.Id != 1 {
		t.Fatal("result not assigned through guarded wrapper")
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT COUNT(*) AS count FROM optional_records"}, []struct{ Count int }{{1}})
}
