package validation_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/mattn/go-sqlite3"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	"github.com/viant/govalidator"
	h "github.com/viant/xdatly/handler"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type allCheckFields struct{}

func (allCheckFields) Has(string) bool { return true }
func TestCheckOnlyBatchIndependentCoverageAndSnapshots(t *testing.T) {
	for _, scalar := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "scalar"}[scalar], func(t *testing.T) {
			db := sqlite.New(t)
			if err := db.ExecStatements(t.Context(), "CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			sqlFields, goFields := fields{"Parent": true}, fields{"Label": true}
			off, on := false, true
			p := checkPolicy(sqlFields)
			p.GoFields = goFields
			p.CheckUnique = &off
			p.CheckRef = &on
			source := &checkConnection{value: validation.Connection{DB: db.DB}, before: func() {
				sqlFields["Parent"] = false
				goFields["Label"] = false
				goFields["Name"] = true
				off = true
				on = false
			}}
			missing := 404
			row := &checkAssociation{Parent: &missing}
			var ret *h.Validation
			var err error
			if scalar {
				ret, err = validation.New(source).Validate(t.Context(), row, p)
			} else {
				ret, err = validation.New(source).Validate(t.Context(), []*checkAssociation{row}, []h.ValidationOptions{p})
			}
			if err != nil || ret == nil || !ret.Failed || len(ret.Violations) != 2 || ret.Violations[0].Check != "required" || ret.Violations[0].Field != "Label" || ret.Violations[1].Check != "refKey" || ret.Violations[1].Field != "Parent" || source.calls != 1 {
				t.Fatalf("result=%+v err=%v calls=%d", ret, err, source.calls)
			}
		})
	}
	for _, tc := range []struct {
		name      string
		goFields  h.FieldSet
		sqlFields fields
		want      int
	}{
		{"nil defaults SQL", nil, fields{}, 0}, {"typed nil defaults SQL", fields(nil), fields{}, 0}, {"empty Go stays empty", fields{}, fields{"Label": true}, 0}, {"all Go independent", allCheckFields{}, fields{}, 1}, {"nil uses selected SQL", nil, fields{"Label": true}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			p := checkPolicy(tc.sqlFields)
			p.GoFields = tc.goFields
			ret, err := dml.NewData(db.DB).FrameworkValidator().Validate(t.Context(), []*checkAssociation{{}}, []h.ValidationOptions{p})
			if err != nil || ret == nil || len(ret.Violations) != tc.want {
				t.Fatalf("ret=%+v err=%v", ret, err)
			}
		})
	}
}
func TestCheckOnlyBatchPreflightAndFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     any
		policies []h.ValidationOptions
	}{
		{"mixed", []*checkAssociation{{}, {}}, []h.ValidationOptions{checkPolicy(fields{}), {Action: h.WriteInsert, Shallow: true}}}, {"mixed reverse", []*checkAssociation{{}, {}}, []h.ValidationOptions{{Action: h.WriteInsert, Shallow: true}, checkPolicy(fields{})}},
		{"count", []*checkAssociation{{}}, []h.ValidationOptions{}}, {"interface slice", []any{&checkAssociation{}}, []h.ValidationOptions{checkPolicy(fields{})}}, {"scalar slice", []int{1}, []h.ValidationOptions{checkPolicy(fields{})}}, {"nil candidate", []*checkAssociation{nil}, []h.ValidationOptions{checkPolicy(fields{})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &noConnection{}
			ret, err := validation.New(source).Validate(t.Context(), tc.rows, tc.policies)
			if err == nil || ret != nil || source.calls != 0 {
				t.Fatalf("ret=%v err=%v calls=%d", ret, err, source.calls)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		change func(*h.ValidationOptions)
	}{
		{"shallow", func(p *h.ValidationOptions) { p.Shallow = false }}, {"unique nil", func(p *h.ValidationOptions) { p.CheckUnique = nil }}, {"unique true", func(p *h.ValidationOptions) { v := true; p.CheckUnique = &v }}, {"SQL fields nil", func(p *h.ValidationOptions) { p.Fields = nil }}, {"SQL fields typed nil", func(p *h.ValidationOptions) { p.Fields = fields(nil) }},
		{"action", func(p *h.ValidationOptions) { p.Action = h.WriteInsert }}, {"previous", func(p *h.ValidationOptions) { p.Previous = &checkAssociation{} }}, {"previous fields", func(p *h.ValidationOptions) { p.PreviousFields = fields{} }}, {"presence", func(p *h.ValidationOptions) { p.HonorPresence = true }}, {"deferrals", func(p *h.ValidationOptions) { p.DeferredFields = fields{} }}, {"references", func(p *h.ValidationOptions) {
			p.SatisfiedReferences = []h.ValidationReference{{Field: "Parent", Table: "parents", Column: "id"}}
		}},
		{"connector mismatch", func(p *h.ValidationOptions) { p.Connector = "other" }}, {"checks mismatch", func(p *h.ValidationOptions) { v := false; p.CheckRef = &v }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &noConnection{}
			first := checkPolicy(fields{})
			second := first
			tc.change(&second)
			ret, err := validation.New(source).Validate(t.Context(), []*checkAssociation{{}, {}}, []h.ValidationOptions{first, second})
			if err == nil || ret != nil || source.calls != 0 {
				t.Fatalf("ret=%v err=%v calls=%d", ret, err, source.calls)
			}
		})
	}
	for _, scalar := range []bool{false, true} {
		t.Run(map[bool]string{false: "mutation GoFields batch", true: "mutation GoFields scalar"}[scalar], func(t *testing.T) {
			source := &noConnection{}
			p := h.ValidationOptions{Action: h.WriteInsert, Shallow: true, GoFields: fields{}}
			var err error
			if scalar {
				_, err = validation.New(source).Validate(t.Context(), &checkAssociation{}, p)
			} else {
				_, err = validation.New(source).Validate(t.Context(), []*checkAssociation{{}}, []h.ValidationOptions{p})
			}
			if err == nil || source.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, source.calls)
			}
		})
	}
	source := &noConnection{}
	empty, err := validation.New(source).Validate(t.Context(), []*checkAssociation(nil), []h.ValidationOptions(nil))
	if err != nil || empty == nil || empty.Failed || source.calls != 0 {
		t.Fatalf("empty=%v err=%v calls=%d", empty, err, source.calls)
	}
	rows := []*checkAssociation{{}}
	policies := []h.ValidationOptions{checkPolicy(fields{})}
	for _, ctx := range []context.Context{nil, func() context.Context { c, cancel := context.WithCancel(t.Context()); cancel(); return c }()} {
		if ret, err := validation.New(source).Validate(ctx, rows, policies); err == nil || ret != nil || source.calls != 0 {
			t.Fatalf("context ret=%v err=%v", ret, err)
		}
	}
	if ret, err := validation.New(nil).Validate(t.Context(), rows, policies); err == nil || ret != nil {
		t.Fatalf("source ret=%v err=%v", ret, err)
	}
	db := sqlite.New(t)
	p := checkPolicy(fields{})
	p.Connector = "unregistered"
	if ret, err := dml.NewData(db.DB).FrameworkValidator().Validate(t.Context(), rows, []h.ValidationOptions{p}); err == nil || ret != nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("connector ret=%v err=%v", ret, err)
	}
	sentinel := errors.New("connection unavailable")
	if ret, err := validation.New(failingCheckSource{sentinel}).Validate(t.Context(), rows, policies); !errors.Is(err, sentinel) || ret != nil {
		t.Fatalf("connection ret=%v err=%v", ret, err)
	}
	p = h.ValidationOptions{Action: h.WriteInsert, Shallow: true, GoFields: fields(nil)}
	if _, err := dml.NewData(db.DB).FrameworkValidator().Validate(t.Context(), &goRow{Name: "ok"}, p); err != nil {
		t.Fatalf("typed-nil mutation GoFields: %v", err)
	}
}

type failingCheckSource struct{ err error }

func (s failingCheckSource) ValidationConnection(context.Context, string) (validation.Connection, error) {
	return validation.Connection{}, s.err
}

type checkBatchGoRow struct {
	ID     int    `sqlx:"id,primaryKey"`
	Label  string `sqlx:"label" validate:"required"`
	Trace  int    `sqlx:"-" validate:"checkonly_batch_predicate"`
	Parent *int   `sqlx:"parent,refTable=parents,refColumn=id"`
}
type checkBatchTraceKey struct{}

var checkOnlyBatchDriverID atomic.Uint64

func TestCheckOnlyBatchAllGoAndPartialFailure(t *testing.T) {
	govalidator.Register("checkonly_batch_predicate", func(*govalidator.Field, *govalidator.Check) (govalidator.IsValid, error) {
		return func(ctx context.Context, _ interface{}) (bool, error) {
			events := ctx.Value(checkBatchTraceKey{}).(*[]string)
			*events = append(*events, "go")
			return true, nil
		}, nil
	})
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid SQL", true: "SQL failure retains Go"}[fail], func(t *testing.T) {
			events := []string{}
			driver := fmt.Sprintf("checkonly-batch-order-%t-%d", fail, checkOnlyBatchDriverID.Add(1))
			sql.Register(driver, &sqlite3.SQLiteDriver{ConnectHook: func(c *sqlite3.SQLiteConn) error {
				c.RegisterAuthorizer(func(op int, table, column, database string) int {
					if op == sqlite3.SQLITE_READ && table == "parents" {
						events = append(events, "sql")
					}
					return sqlite3.SQLITE_OK
				})
				return nil
			}})
			db, openErr := sql.Open(driver, filepath.Join(t.TempDir(), "order.db"))
			if openErr != nil {
				t.Fatal(openErr)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if !fail {
				if _, err := db.Exec("CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
					t.Fatal(err)
				}
			}
			events = nil
			ctx := context.WithValue(t.Context(), checkBatchTraceKey{}, &events)
			miss := 404
			rows := []*checkBatchGoRow{{Parent: &miss, Trace: 1}, {Label: "ok", Parent: &miss, Trace: 2}}
			p := checkPolicy(fields{"Parent": true})
			p.GoFields = fields{"Label": true, "Trace": true}
			p.Location = "Data[0]"
			q := p
			q.Location = "Data[1]"
			var before, after int
			if err := db.QueryRow("SELECT total_changes()").Scan(&before); err != nil {
				t.Fatal(err)
			}
			ret, err := dml.NewData(db).FrameworkValidator().Validate(ctx, rows, []h.ValidationOptions{p, q})
			if len(events) < 2 || !reflect.DeepEqual(events[:2], []string{"go", "go"}) {
				t.Fatalf("Go predicates=%v", events)
			}
			if ret == nil || !ret.Failed || ret.Violations[0].Location != "Data[0].Label" || ret.Violations[0].Check != "required" {
				t.Fatalf("ret=%+v err=%v", ret, err)
			}
			if !fail && (len(events) < 3 || events[2] != "sql") {
				t.Fatalf("Go/SQL order=%v", events)
			}
			if fail {
				if err == nil || len(ret.Violations) != 1 {
					t.Fatalf("partial SQL escaped: %+v %v", ret, err)
				}
			} else {
				if err != nil || len(ret.Violations) != 3 || ret.Violations[1].Check != "refKey" || ret.Violations[2].Location != "Data[1].Parent" {
					t.Fatalf("batch locations: %+v %v", ret, err)
				}
			}
			if e := db.QueryRow("SELECT total_changes()").Scan(&after); e != nil || after != before {
				t.Fatalf("changes %d->%d err=%v", before, after, e)
			}
		})
	}
}

func TestCheckOnlyBatchSQLFailureWithoutInventedDiagnostics(t *testing.T) {
	db := sqlite.New(t)
	missing := 404
	row := &checkAssociation{Parent: &missing}
	p := checkPolicy(fields{"Parent": true})
	p.GoFields = fields{}
	service := dml.NewData(db.DB).FrameworkValidator()
	ret, err := service.Validate(t.Context(), []*checkAssociation{row}, []h.ValidationOptions{p})
	if err == nil || ret == nil || ret.Failed || len(ret.Violations) != 0 {
		t.Fatalf("invented partial result: %+v %v", ret, err)
	}
	if ret, err = service.Validate(t.Context(), row, p); err == nil || ret != nil {
		t.Fatalf("scalar error behavior changed: %+v %v", ret, err)
	}
	mutation := h.ValidationOptions{Action: h.WriteInsert, Shallow: true}
	if ret, err = service.Validate(t.Context(), []*checkAssociation{row}, []h.ValidationOptions{mutation}); err == nil || ret != nil {
		t.Fatalf("mutation error behavior changed: %+v %v", ret, err)
	}
}
