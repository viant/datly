package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/provider"
	sqldml "github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

type reportedRow struct {
	ID   int    `sqlx:"id,primaryKey"`
	Name string `sqlx:"name"`
}

func TestInjectedMutationReporterObservesExecutedChildSQLite(t *testing.T) {
	for _, tc := range []struct {
		name     string
		id       int
		affected int64
	}{
		{"changed", 1, 1}, {"ignored", 1, 0}, {"missing", 99, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE items(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO items VALUES(1,'old')",
				"CREATE TRIGGER ignore_update BEFORE UPDATE ON items WHEN NEW.name='ignored' BEGIN SELECT RAISE(IGNORE); END"); err != nil {
				t.Fatal(err)
			}
			source := sqldml.Source{DB: h.DB}
			input := testRouteInput(t, reflect.TypeFor[struct{}]())
			_, err := New().Execute(ctx, Request{Input: input, BoundInput: &struct{}{}, DataSource: source,
				Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					deps := struct {
						Reporter dexec.MutationReporter `bind:"kind=mutationReporter,required"`
					}{}
					if err := invocation.Binder.Bind(ctx, &deps); err != nil {
						return nil, err
					}
					if _, unsafe := deps.Reporter.(xhandler.Flusher); unsafe {
						return nil, fmt.Errorf("reporter exposes execution")
					}
					if _, unsafe := deps.Reporter.(interface {
						Complete(context.Context, error) error
					}); unsafe {
						return nil, fmt.Errorf("reporter exposes completion")
					}
					before := deps.Reporter.MutationReport()
					if before.Queued != 0 || len(before.Results) != 0 {
						return nil, fmt.Errorf("unexpected pre-write evidence: %+v", before)
					}
					_, err := New().Execute(PrepareComponent(ctx, ComponentImperative, ""), Request{Input: input, BoundInput: &struct{}{}, DataSource: source,
						Handler: rhandler.HandlerFunc(func(ctx context.Context, child rhandler.Invocation) (any, error) {
							writes := struct {
								DML   xhandler.DML     `bind:"kind=dml,required"`
								Flush xhandler.Flusher `bind:"kind=flusher,required"`
							}{}
							if err := child.Binder.Bind(ctx, &writes); err != nil {
								return nil, err
							}
							if err := writes.DML.Update("items", &reportedRow{ID: tc.id, Name: tc.name}); err != nil {
								return nil, err
							}
							return nil, writes.Flush.Flush(ctx, "")
						})})
					if err != nil {
						return nil, err
					}
					report := deps.Reporter.MutationReport()
					if report.Queued != 1 || !report.Nested || len(report.Results) != 1 || report.Results[0].Operation != "update" || report.Results[0].Table != "items" || report.Results[0].Affected != tc.affected || report.Results[0].Error != nil {
						return nil, fmt.Errorf("executed child evidence: %+v", report)
					}
					report.Results[0].Affected = 99
					if deps.Reporter.MutationReport().Results[0].Affected != tc.affected {
						return nil, fmt.Errorf("report aliases execution evidence")
					}
					return nil, nil
				})})
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if tc.affected == 1 {
				want = tc.name
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM items"}, []reportedRow{{ID: 1, Name: want}})
		})
	}
}

func TestMutationReporterAuthorityIsProtected(t *testing.T) {
	fake := []locator.Provider{provider.Static(dexec.MutationReporterKey, struct{}{})}
	for _, input := range []providerComposition{{component: fake}, {protocol: fake}, {child: fake}} {
		if _, err := (providerComposer{}).compose(input); err == nil {
			t.Fatal("untrusted mutation evidence provider accepted")
		}
	}
}
