package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func TestSequenceStrategySharesRootAndCallerTransaction(t *testing.T) {
	for _, childSetting := range []string{"", "reservation", "transient"} {
		t.Run(childSetting, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			h.DB.SetMaxOpenConns(1)
			if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT)"); err != nil {
				t.Fatal(err)
			}
			tx, err := h.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			root, _ := invocationDataScope(ctx, dml.Source{DB: h.DB, Tx: tx})
			root.sequenceStrategy = "reservation"
			rootData, err := root.resolve(ctx)
			if err != nil {
				t.Fatal(err)
			}
			child, _ := invocationDataScope(withDataScope(ctx, root), dml.Source{DB: h.DB})
			child.sequenceStrategy = childSetting
			data, err := child.resolve(ctx)
			if childSetting == "transient" {
				if !errors.Is(err, ErrSequenceStrategyConflict) {
					t.Fatalf("root policy silently overridden: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				type row struct {
					ID int64 `sqlx:"id,primaryKey,autoincrement"`
				}
				a, b := &row{}, &row{}
				if err = rootData.Allocate(ctx, "records", a, "ID"); err != nil {
					t.Fatal(err)
				}
				if err = data.Allocate(ctx, "records", b, "ID"); err != nil {
					t.Fatal(err)
				}
				if a.ID != 1 || b.ID != 2 || child.unit != root {
					t.Fatal("strategy created a second data/transaction owner")
				}
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal("caller transaction taken over", err)
			}
		})
	}
}

func TestSequenceStrategyOmittedRootUsesNativeDefaultBeforeOpen(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	events := []string{}
	audit := &strategySourceAudit{Source: dml.Source{DB: h.DB}, events: &events}
	root, _ := invocationDataScope(ctx, audit)
	rootData, err := root.resolve(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rootData == nil || root.sequenceResolved != "reservation" || fmt.Sprint(events) != "[resolve reservation open reservation]" {
		t.Fatalf("root opened before native policy was frozen: policy=%q events=%v data=%T", root.sequenceResolved, events, rootData)
	}
	opened := root.data

	matching, _ := invocationDataScope(withDataScope(ctx, root), dml.Source{DB: h.DB})
	matching.sequenceStrategy = "reservation"
	if data, resolveErr := matching.resolve(ctx); resolveErr != nil || data == nil || matching.unit != root {
		t.Fatalf("explicit native default did not share omitted root: data=%T unit=%p root=%p err=%v", data, matching.unit, root, resolveErr)
	}

	conflicting, _ := invocationDataScope(withDataScope(ctx, root), dml.Source{DB: h.DB})
	conflicting.sequenceStrategy = "transient"
	if _, resolveErr := conflicting.resolve(ctx); !errors.Is(resolveErr, ErrSequenceStrategyConflict) {
		t.Fatalf("differing child policy did not conflict: %v", resolveErr)
	}
	if root.data != opened || root.sequenceResolved != "reservation" || fmt.Sprint(events) != "[resolve reservation open reservation]" {
		t.Fatalf("child switched or reopened root policy: policy=%q events=%v data changed=%v", root.sequenceResolved, events, root.data != opened)
	}

	laterMatch, _ := invocationDataScope(withDataScope(ctx, root), dml.Source{DB: h.DB})
	laterMatch.sequenceStrategy = "reservation"
	if _, resolveErr := laterMatch.resolve(ctx); resolveErr != nil || laterMatch.unit != root {
		t.Fatalf("matching policy failed after rejected conflict: %v", resolveErr)
	}
	if completeErr := root.complete(ctx, errors.New("test cleanup")); completeErr == nil {
		t.Fatal("cleanup cause was lost")
	}
}

type strategySourceAudit struct {
	dml.Source
	events *[]string
}

func (s *strategySourceAudit) Open(ctx context.Context) (xhandler.Data, error) {
	*s.events = append(*s.events, "open", s.InvocationSequenceStrategy())
	return s.Source.Open(ctx)
}

func (s *strategySourceAudit) ResolveSequenceStrategy(ctx context.Context) (dexec.DataSource, string, error) {
	resolved, policy, err := s.Source.ResolveSequenceStrategy(ctx)
	if err != nil {
		return nil, "", err
	}
	prepared := resolved.(dml.Source)
	*s.events = append(*s.events, "resolve", policy)
	return &strategySourceAudit{Source: prepared, events: s.events}, policy, nil
}

func (s *strategySourceAudit) WithSequenceStrategy(strategy string) (dexec.DataSource, error) {
	configured, err := s.Source.WithSequenceStrategy(strategy)
	if err != nil {
		return nil, err
	}
	return &strategySourceAudit{Source: configured.(dml.Source), events: s.events}, nil
}
func TestSequenceStrategyRejectsReusedDatabaseUnitConflict(t *testing.T) {
	h := sqlite.New(t)
	root := &dataScope{bySource: map[any]*dataScope{}}
	ctx := context.Background()
	first, err := root.databaseUnit(ctx, dml.Source{DB: h.DB}, h.DB, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = first.resolve(ctx); err != nil || first.sequenceResolved != "reservation" {
		t.Fatalf("omitted unit did not freeze native default before Open: policy=%q err=%v", first.sequenceResolved, err)
	}
	opened := first.data
	same, err := root.databaseUnit(ctx, dml.Source{DB: h.DB}, h.DB, "reservation")
	if err != nil || same != first {
		t.Fatal("explicit matching native default did not reuse the unit", err)
	}
	if _, err = root.databaseUnit(ctx, dml.Source{DB: h.DB}, h.DB, "transient"); !errors.Is(err, ErrSequenceStrategyConflict) {
		t.Fatal("unit policy overwritten", err)
	}
	if first.data != opened || first.sequenceResolved != "reservation" {
		t.Fatal("reused unit changed after Open")
	}
	_ = first.complete(ctx, errors.New("test cleanup"))
}
