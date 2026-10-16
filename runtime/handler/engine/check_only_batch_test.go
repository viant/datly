package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/custom"
	sqldml "github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
	modernresponse "github.com/viant/xdatly/response"
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

func TestScopedFactoryDataSourceRootAndCallerCompletion(t *testing.T) {
	for _, caller := range []bool{false, true} {
		for _, mode := range []string{"success", "Go violation", "SQL failure", "handler error", "late SQL failure", "caller error", "default failure", "lazy failure"} {
			if !caller && mode == "caller error" {
				continue
			}
			t.Run(fmt.Sprintf("caller=%v/%s", caller, mode), func(t *testing.T) {
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
				order := []string{}
				fake := &frameworkAlwaysValid{}
				sentinel := errors.New("handler source failure")
				publicBody := &scopedOutput{Value: "rejected"}
				publicErr := &modernresponse.Error{Code: 403, Cause: sentinel, Payload: publicBody}
				off, on := false, true
				one := 1
				rows := []*checkOnlyBatchRow{{ID: 1, Name: "first", Parent: &one}, {ID: 2, Name: "second", Parent: &one}}
				if mode == "Go violation" {
					rows[0].Name = ""
				}
				calls := 0
				c := &scopedContract{run: func(ctx context.Context, s h.Session, i *scopedInput, o *scopedOutput) error {
					calls++
					order = append(order, "begin")
					if i.Credential == nil || i.Child != nil || i.Has == nil || !i.Has.Credential || i.Has.Child {
						return fmt.Errorf("default/lazy state changed")
					}
					marker := i.Has
					if err := s.Binder().(h.ScopedBinder).BindScope(ctx, i, "lazy"); err != nil {
						return err
					}
					if i.Child == nil || i.Has != marker || !i.Has.Child {
						return fmt.Errorf("canonical lazy marker changed")
					}
					d, found, err := s.Binder().Lookup(ctx, h.DMLKey)
					if err != nil || !found {
						return fmt.Errorf("DML missing: %v", err)
					}
					v, found, err := s.Binder().Lookup(ctx, h.FrameworkValidatorKey)
					if err != nil || !found {
						return fmt.Errorf("validator missing: %v", err)
					}
					if err := d.(h.DML).Insert("audit", &checkOnlyBatchAudit{ID: 1}); err != nil {
						return err
					}
					p := h.ValidationOptions{CheckOnly: true, Shallow: true, Fields: checkOnlyBatchFields{"Parent": true}, GoFields: checkOnlyBatchFields{"Name": true}, CheckUnique: &off, CheckRef: &on, Location: "Data[0]"}
					q := p
					q.Location = "Data[1]"
					result, validationErr := v.(h.Validator).Validate(ctx, rows, []h.ValidationOptions{p, q})
					var n int
					if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&n); err != nil || n != 0 {
						return fmt.Errorf("early drain %d: %v", n, err)
					}
					order = append(order, "validated")
					if validationErr != nil {
						return validationErr
					}
					if result == nil {
						return fmt.Errorf("nil validation")
					}
					if err := result.Err(); err != nil {
						return err
					}
					if mode == "handler error" {
						return publicErr
					}
					if mode == "late SQL failure" {
						if err := d.(h.DML).Insert("missing_audit", &checkOnlyBatchAudit{ID: 2}); err != nil {
							return err
						}
					}
					o.Value = "done"
					return nil
				}}
				factory := custom.Factory[scopedInput, scopedOutput](func() h.Contract[scopedInput, scopedOutput] { return c })
				adapted, err := factory()
				if err != nil {
					t.Fatal(err)
				}
				credential := "allowed"
				if mode == "default failure" {
					credential = ""
				}
				var lazyErr error
				if mode == "lazy failure" {
					lazyErr = sentinel
				}
				req := scopedRequest(t, c, credential, &order, lazyErr)
				req.Handler = adapted
				req.DataSource = source
				req.Capabilities.Validator = fake
				ctx := t.Context()
				var root *dataScope
				if caller {
					var owns bool
					root, owns = invocationDataScope(ctx, source)
					if !owns {
						t.Fatal("caller not owner")
					}
					ctx = withDataScope(ctx, root)
					if _, err := root.resolve(ctx); err != nil {
						t.Fatal(err)
					}
					if err := data.Insert("audit", &checkOnlyBatchAudit{ID: 90}); err != nil {
						t.Fatal(err)
					}
					req.BufferedComponentCalls = true
					ctx = PrepareComponent(ctx, ComponentBufferedImperative, "")
				}
				result, operationErr := New().Execute(ctx, req)
				if caller {
					var pending int
					if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&pending); err != nil || pending != 0 {
						t.Fatalf("child drain %d %v", pending, err)
					}
					if mode == "caller error" {
						operationErr = sentinel
					}
					result, operationErr = finishDataScope(ctx, root, true, result, operationErr)
				}
				if mode == "success" {
					if operationErr != nil || result.(*scopedOutput).Value != "done" {
						t.Fatalf("%v %v", result, operationErr)
					}
				} else if operationErr == nil {
					t.Fatalf("failure continued: %v", result)
				}
				if mode == "handler error" && !errors.Is(operationErr, sentinel) {
					t.Fatal("cause lost", operationErr)
				}
				if mode == "handler error" {
					var got *modernresponse.Error
					if !errors.As(operationErr, &got) || got.Code != 403 || got.Payload != publicBody {
						t.Fatalf("public ownership changed: %v", operationErr)
					}
				}
				if mode == "late SQL failure" && result != nil {
					t.Fatalf("completion error retained success: %v", result)
				}
				wantOrder := "default,begin,child,validated"
				if mode == "default failure" {
					wantOrder = "default"
				}
				if mode == "lazy failure" {
					wantOrder = "default,begin,child"
				}
				if strings.Join(order, ",") != wantOrder || fake.calls != 0 {
					t.Fatalf("order=%v fake=%d", order, fake.calls)
				}
				wantCalls := 1
				if mode == "default failure" {
					wantCalls = 0
				}
				if calls != wantCalls {
					t.Fatalf("handler calls=%d", calls)
				}
				var count int
				wantCount := 0
				if mode == "success" {
					wantCount = 1
					if caller {
						wantCount = 2
					}
				}
				if err := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); err != nil || count != wantCount {
					t.Fatalf("committed count=%d want%d err=%v operation=%v", count, wantCount, err, operationErr)
				}
				expectedOpens := 1
				if !caller && (mode == "default failure" || mode == "lazy failure") {
					expectedOpens = 0
				}
				if source.opens != expectedOpens {
					t.Fatalf("source opens=%d want%d", source.opens, expectedOpens)
				}
			})
		}
	}
}
