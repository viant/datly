package engine

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/custom"
	sqldml "github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	"strings"
	"testing"
)

type checkOnlyBatchFields map[string]bool

func (f checkOnlyBatchFields) Has(name string) bool { return f[name] }

type checkOnlyBatchRow struct {
	ID     int    `sqlx:"id,primaryKey"`
	Name   string `sqlx:"name" validate:"required"`
	Parent *int   `sqlx:"parent,refTable=parents,refColumn=id"`
}
type checkOnlyBatchAudit struct {
	ID int `sqlx:"id,primaryKey"`
}

func TestScopedFactoryCheckOnlyBatchProtectedBuffer(t *testing.T) {
	for _, mode := range []string{"success", "Go violation", "SQL failure"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			if err := db.ExecStatements(t.Context(), "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "INSERT INTO parents VALUES(1)", "CREATE TABLE audit(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			if mode == "SQL failure" {
				if err := db.ExecStatements(t.Context(), "DROP TABLE parents"); err != nil {
					t.Fatal(err)
				}
			}
			data := sqldml.NewData(db.DB)
			source := &staticDataSource{data: data}
			fake := &frameworkAlwaysValid{}
			order := []string{}
			off, on := false, true
			one := 1
			rows := []*checkOnlyBatchRow{{ID: 1, Name: "first", Parent: &one}, {ID: 2, Name: "second", Parent: &one}}
			if mode != "success" {
				rows[0].Name = ""
			}
			c := &scopedContract{run: func(ctx context.Context, s h.Session, i *scopedInput, o *scopedOutput) error {
				order = append(order, "begin")
				if i.Credential == nil || i.Child != nil || i.Has == nil || !i.Has.Credential || i.Has.Child {
					return fmt.Errorf("default/lazy state changed")
				}
				if err := s.Binder().(h.ScopedBinder).BindScope(ctx, i, "lazy"); err != nil {
					return err
				}
				if i.Child == nil || !i.Has.Child {
					return fmt.Errorf("lazy binding missing")
				}
				d, found, err := s.Binder().Lookup(ctx, h.DMLKey)
				if err != nil || !found {
					return fmt.Errorf("DML missing: %v", err)
				}
				v, found, err := s.Binder().Lookup(ctx, h.FrameworkValidatorKey)
				if err != nil || !found {
					return fmt.Errorf("protected validator missing: %v", err)
				}
				if err := d.(h.DML).Insert("audit", &checkOnlyBatchAudit{ID: 1}); err != nil {
					return err
				}
				p := h.ValidationOptions{CheckOnly: true, Shallow: true, Fields: checkOnlyBatchFields{"Parent": true}, GoFields: checkOnlyBatchFields{"Name": true}, CheckUnique: &off, CheckRef: &on, Location: "Data[0]"}
				q := p
				q.Location = "Data[1]"
				result, validationErr := v.(h.Validator).Validate(ctx, rows, []h.ValidationOptions{p, q})
				var count int
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != 0 {
					return fmt.Errorf("validation flushed caller buffer: count=%d err=%v", count, err)
				}
				if result == nil {
					return fmt.Errorf("validation lost partial result: %v", validationErr)
				}
				if mode != "success" && (!result.Failed || len(result.Violations) != 1 || result.Violations[0].Location != "Data[0].Name") {
					return fmt.Errorf("wrong diagnostics: %+v", result)
				}
				order = append(order, "validated")
				if validationErr != nil {
					return validationErr
				}
				if err := result.Err(); err != nil {
					return err
				}
				o.Value = "done"
				return nil
			}}
			factory := custom.Factory[scopedInput, scopedOutput](func() h.Contract[scopedInput, scopedOutput] { return c })
			adapted, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			request := scopedRequest(t, c, "allowed", &order, nil)
			request.Handler = adapted
			root := newDataScope(source)
			ctx := withDataScope(t.Context(), root)
			request.Capabilities.Validator = fake
			result, err := New().Execute(ctx, request)
			var pending int
			if e := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&pending); e != nil || pending != 0 {
				t.Fatalf("child completed caller buffer: count=%d err=%v", pending, e)
			}
			completionErr := completeDataScope(ctx, root, true, err)
			if err == nil {
				err = completionErr
			}
			if mode == "success" {
				if err != nil || result.(*scopedOutput).Value != "done" {
					t.Fatalf("result=%v err=%v", result, err)
				}
			} else if err == nil {
				t.Fatal("validation failure continued")
			}
			if strings.Join(order, ",") != "default,begin,child,validated" || source.opens != 1 || fake.calls != 0 {
				t.Fatalf("order=%v opens=%d fake=%d err=%v", order, source.opens, fake.calls, err)
			}
			var count int
			if e := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); e != nil || count != map[bool]int{true: 1, false: 0}[mode == "success"] {
				t.Fatalf("final count=%d err=%v", count, e)
			}
		})
	}
}
