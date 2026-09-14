package dml

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
)

func TestInvocationOrdersParentBeforeBindingChildren(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx,
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE parent (id INTEGER PRIMARY KEY)",
		"CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))",
	); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	child := data.ComponentData(ComponentBinding, "00000000").(*Data)
	if err := child.Execute("INSERT INTO child(id, parent_id) VALUES (1, 10)"); err != nil {
		t.Fatal(err)
	}
	child.SealComponent()
	if err := data.Execute("INSERT INTO parent(id) VALUES (10)"); err != nil {
		t.Fatal(err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM child").Scan(&count); err != nil || count != 1 {
		t.Fatalf("child count=%d err=%v", count, err)
	}
}

func TestInvocationPreservesImperativeCallPositionAndBindingOrder(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit (position INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	_ = data.Execute("INSERT INTO audit(name) VALUES ('parent-before')")
	imperative := data.ComponentData(ComponentImperative, "").(*Data)
	_ = imperative.Execute("INSERT INTO audit(name) VALUES ('imperative')")
	imperative.SealComponent()
	_ = data.Execute("INSERT INTO audit(name) VALUES ('parent-after')")
	third := data.ComponentData(ComponentBinding, "00000002").(*Data)
	_ = third.Execute("INSERT INTO audit(name) VALUES ('third')")
	third.SealComponent()
	first := data.ComponentData(ComponentBinding, "00000000").(*Data)
	_ = first.Execute("INSERT INTO audit(name) VALUES ('first')")
	first.SealComponent()
	second := data.ComponentData(ComponentBinding, "00000001").(*Data)
	_ = second.Execute("INSERT INTO audit(name) VALUES ('second')")
	second.SealComponent()
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	rows, err := h.DB.QueryContext(ctx, "SELECT name FROM audit ORDER BY position")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var actual []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		actual = append(actual, name)
	}
	want := []string{"parent-before", "imperative", "parent-after", "first", "second", "third"}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("order=%v want %v", actual, want)
	}
}

func TestInvocationFlushIsCausalAndRootFailureRollsItBack(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	binding := data.ComponentData(ComponentBinding, "00000000").(*Data)
	_ = binding.Execute("INSERT INTO audit(id) VALUES (3)")
	binding.SealComponent()
	_ = data.Execute("INSERT INTO audit(id) VALUES (1)")
	imperative := data.ComponentData(ComponentImperative, "").(*Data)
	_ = imperative.Execute("INSERT INTO audit(id) VALUES (2)")
	if err := imperative.Flush(ctx, ""); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := data.tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&pending); err != nil || pending != 2 {
		t.Fatalf("imperative flush crossed binding boundary: pending=%d err=%v", pending, err)
	}
	imperative.SealComponent()
	expected := errors.New("parent failed")
	if err := data.Complete(ctx, expected); !errors.Is(err, expected) {
		t.Fatalf("Complete() error=%v", err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestBindingChildCannotFlushOpenAncestor(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	child := data.ComponentData(ComponentBinding, "00000000").(*Data)
	_ = child.Execute("INSERT INTO audit(id) VALUES (1)")
	if err := child.Flush(ctx, ""); err == nil {
		t.Fatal("expected binding flush rejection")
	}
	child.SealComponent()
	_ = data.Complete(ctx, errors.New("abort"))
}

func TestImperativeDescendantCannotFlushInsideOpenBinding(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	binding := data.ComponentData(ComponentBinding, "00000000").(*Data)
	imperative := binding.ComponentData(ComponentImperative, "").(*Data)
	_ = imperative.Execute("INSERT INTO audit(id) VALUES (1)")
	if err := imperative.Flush(ctx, ""); !errors.Is(err, ErrBindingFlush) {
		t.Fatalf("Flush() error=%v", err)
	}
	_ = data.Complete(ctx, errors.New("abort"))
}

func TestInvocationSequencingUsesRootTransaction(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE EMP (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT)"); err != nil {
		t.Fatal(err)
	}
	type employee struct {
		ID   int64  `sqlx:"ID,primaryKey=true"`
		Name string `sqlx:"NAME"`
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	values := []*employee{{Name: "reserved"}}
	if err := data.Allocate(ctx, "EMP", values, "ID"); err != nil {
		t.Fatal(err)
	}
	expected := errors.New("rollback root")
	if err := data.Complete(ctx, expected); !errors.Is(err, expected) {
		t.Fatalf("Complete() error=%v", err)
	}
	result, err := h.DB.ExecContext(ctx, "INSERT INTO EMP(NAME) VALUES ('actual')")
	if err != nil {
		t.Fatal(err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("sequence allocation escaped root transaction: next id=%d", id)
	}
}

func TestInvocationSupportsConcurrentAppend(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for i := 1; i <= 100; i++ {
		id := i
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := data.Execute(fmt.Sprintf("INSERT INTO audit(id) VALUES (%d)", id)); err != nil {
				t.Errorf("Execute() error=%v", err)
			}
		}()
	}
	wait.Wait()
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := h.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 100 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if err := data.Execute("INSERT INTO audit(id) VALUES (101)"); !errors.Is(err, ErrInvocationCompleted) {
		t.Fatalf("stale append error=%v", err)
	}
}

func TestExecutedOperationsRemainInJournalWhileLaterAppendStaysPending(t *testing.T) {
	data := NewData(nil)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	binding := data.ComponentData(ComponentBinding, "00000000").(*Data)
	_ = binding.Execute("binding")
	binding.SealComponent()
	_ = data.Execute("parent-before")
	matched := data.operations("", nil)
	_ = data.Execute("parent-appended-during-flush")
	for _, operation := range matched {
		operation.executed = true
	}
	pending := data.operations("", nil)
	if len(pending) != 1 || pending[0].dml != "parent-appended-during-flush" {
		t.Fatalf("pending=%+v", pending)
	}
	journal := flattenData(data)
	if len(journal) != 3 || !journal[0].executed || journal[1].executed || !journal[2].executed {
		t.Fatalf("journal=%+v", journal)
	}
}

func TestCompleteWaitsForInFlightExecution(t *testing.T) {
	data := NewData(nil)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	data.executionMu.Lock()
	started := make(chan struct{})
	done := make(chan error, 1)
	wantErr := errors.New("abort")
	go func() {
		close(started)
		done <- data.Complete(context.Background(), wantErr)
	}()
	<-started
	select {
	case err := <-done:
		data.executionMu.Unlock()
		t.Fatalf("Complete returned while execution lock was held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	data.executionMu.Unlock()
	if err := <-done; !errors.Is(err, wantErr) {
		t.Fatalf("Complete() error=%v", err)
	}
}

func TestSealedComponentRejectsLateAppend(t *testing.T) {
	data := NewData(nil)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	child := data.ComponentData(ComponentImperative, "").(*Data)
	child.SealComponent()
	if err := child.Execute("late"); !errors.Is(err, ErrComponentSealed) {
		t.Fatalf("Execute() error=%v", err)
	}
}

func TestRepeatedDispatchFromSealedComponentReparentsToOpenAncestor(t *testing.T) {
	data := NewData(nil)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	first := data.ComponentData(ComponentImperative, "").(*Data)
	_ = first.Execute("first")
	first.SealComponent()
	_ = data.Execute("between")

	second := first.ComponentData(ComponentImperative, "").(*Data)
	if second.parent != data {
		t.Fatalf("repeated dispatch parent = %p, want root %p", second.parent, data)
	}
	_ = second.Execute("second")
	second.SealComponent()

	journal := flattenData(data)
	if len(journal) != 3 || journal[0].dml != "first" || journal[1].dml != "between" || journal[2].dml != "second" {
		t.Fatalf("journal order = %+v", journal)
	}
}
