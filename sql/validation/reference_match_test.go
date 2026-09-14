package validation_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

type referenceConnection struct{ connection validation.Connection }

func (s referenceConnection) ValidationConnection(context.Context, string) (validation.Connection, error) {
	return s.connection, nil
}

func TestMatchReferenceUsesNativeIdentityAndSQLValues(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	service := validation.New(referenceConnection{validation.Connection{DB: h.DB, Tx: tx}})
	type childRow struct {
		Parent *int `sqlx:"parent_id,refTable=parents,refColumn=id"`
	}
	type parentRow struct {
		ID sql.NullInt64 `sqlx:"id,primaryKey"`
	}
	zero, one := 0, 1
	ref := xhandler.ValidationReference{Field: "Parent", Table: "parents", Column: "id"}
	for _, tc := range []struct {
		name          string
		child, parent any
		expected      xhandler.ValidationReference
		match, fail   bool
	}{
		{"zero", &childRow{&zero}, &parentRow{sql.NullInt64{Int64: 0, Valid: true}}, ref, true, false},
		{"one", &childRow{&one}, &parentRow{sql.NullInt64{Int64: 1, Valid: true}}, ref, true, false},
		{"different SQL values", &childRow{&one}, &parentRow{sql.NullInt64{Int64: 0, Valid: true}}, ref, false, true},
		{"child NULL", &childRow{}, &parentRow{sql.NullInt64{Valid: true}}, ref, false, true},
		{"parent NULL", &childRow{&zero}, &parentRow{}, ref, false, true},
		{"missing parent column", &childRow{&zero}, &struct{ Other int }{}, ref, false, true},
		{"nil child", (*childRow)(nil), &parentRow{}, ref, false, true},
		{"nil parent", &childRow{&zero}, (*parentRow)(nil), ref, false, true},
		{"wrong target", &childRow{&zero}, &parentRow{}, xhandler.ValidationReference{Field: "Parent", Table: "other", Column: "id"}, false, false},
		{"wrong schema", &childRow{&zero}, &parentRow{}, xhandler.ValidationReference{Field: "Parent", Schema: "other", Table: "parents", Column: "id"}, false, false},
		{"wrong field", &childRow{&zero}, &parentRow{}, xhandler.ValidationReference{Field: "Other", Table: "parents", Column: "id"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := service.MatchReference(ctx, tc.child, tc.parent, tc.expected)
			if (err != nil) != tc.fail || (actual != nil) != tc.match {
				t.Fatalf("match=%+v err=%v", actual, err)
			}
			if actual != nil && *actual != ref {
				t.Fatalf("constraint identity changed: %+v", actual)
			}
		})
	}
	withoutTx := validation.New(referenceConnection{validation.Connection{DB: h.DB}})
	if _, err := withoutTx.MatchReference(ctx, &childRow{&zero}, &parentRow{}, ref); err == nil {
		t.Fatal("matching without active transaction accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := service.MatchReference(cancelled, &childRow{&zero}, &parentRow{}, ref); err != context.Canceled {
		t.Fatalf("cancellation=%v", err)
	}
}

func TestMatchReferenceDataLifetime(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	data := dml.NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	defer data.Complete(ctx, nil)
	matcher, ok := data.FrameworkValidator().(interface {
		MatchReference(context.Context, any, any, xhandler.ValidationReference) (*xhandler.ValidationReference, error)
	})
	if !ok {
		t.Fatal("scoped framework validator lacks reference matching")
	}
	child := &struct {
		Parent int `sqlx:"parent_id,refTable=parents,refColumn=id"`
	}{Parent: 7}
	parent := &struct {
		ID int `sqlx:"id,primaryKey"`
	}{ID: 7}
	ref := xhandler.ValidationReference{Field: "Parent", Table: "parents", Column: "id"}
	if _, err := matcher.MatchReference(ctx, child, parent, ref); err == nil {
		t.Fatal("receipt match before transaction accepted")
	}
	if err := data.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if match, err := matcher.MatchReference(ctx, child, parent, ref); err != nil || match == nil {
		t.Fatalf("active match=%+v error=%v", match, err)
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := matcher.MatchReference(ctx, child, parent, ref); err == nil {
		t.Fatal("completed invocation allowed match")
	}
}

func TestMatchReferenceQualifiedTarget(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	s := validation.New(referenceConnection{validation.Connection{DB: h.DB, Tx: tx}})
	child := &struct {
		Parent int `sqlx:"parent_id,refDb=main,refTable=parents,refColumn=id"`
	}{7}
	parent := &struct {
		ID int `sqlx:"id"`
	}{7}
	for _, tc := range []struct {
		schema, table string
		want          bool
	}{{"main", "parents", true}, {"", "main.parents", true}, {"", "parents", false}, {"other", "parents", false}, {"", "other.parents", false}} {
		actual, err := s.MatchReference(ctx, child, parent, xhandler.ValidationReference{Field: "Parent", Schema: tc.schema, Table: tc.table, Column: "id"})
		if err != nil || (actual != nil) != tc.want {
			t.Fatalf("%s/%s: match=%+v err=%v", tc.schema, tc.table, actual, err)
		}
		if actual != nil && (actual.Schema != "main" || actual.Table != "parents") {
			t.Fatalf("native descriptor changed: %+v", actual)
		}
	}
}
