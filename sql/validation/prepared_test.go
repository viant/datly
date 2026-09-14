package validation_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/validation"
	"github.com/viant/govalidator"
	xhandler "github.com/viant/xdatly/handler"
)

type callbackSource struct {
	db       *sql.DB
	callback func()
	calls    int
}

func (s *callbackSource) ValidationConnection(context.Context, string) (validation.Connection, error) {
	s.calls++
	if s.callback != nil {
		s.callback()
	}
	return validation.Connection{DB: s.db}, nil
}

type preparedResolverRow struct {
	ID    int    `sqlx:"id,primaryKey"`
	Value string `sqlx:"value" validate:"datly_prepared_resolver"`
}

func TestFrameworkPreparedMetadataIsExecutionAuthority(t *testing.T) {
	dependencies := []string{"ID"}
	factories, resolvers, predicates := 0, 0, 0
	govalidator.RegisterWithDependencies("datly_prepared_resolver", func(*govalidator.Field, *govalidator.Check) (govalidator.IsValid, error) {
		factories++
		return func(context.Context, interface{}) (bool, error) { predicates++; return true, nil }, nil
	}, func(*govalidator.Field, *govalidator.Check) ([]string, error) { resolvers++; return dependencies, nil })
	h := sqlite.New(t)
	source := &callbackSource{db: h.DB, callback: func() { dependencies[0] = "Missing" }}
	result, err := validation.New(source).Validate(context.Background(), []*preparedResolverRow{{ID: 1}, {ID: 2}}, []xhandler.ValidationOptions{
		{Action: xhandler.WriteInsert, Shallow: true},
		{Action: xhandler.WriteInsert, Shallow: true, DeferredFields: fields{"ID": true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed || source.calls != 1 || factories != 1 || resolvers != 1 || predicates != 1 || dependencies[0] != "Missing" {
		t.Fatalf("result=%v source=%d factories=%d resolvers=%d predicates=%d dependencies=%v", result, source.calls, factories, resolvers, predicates, dependencies)
	}
}

type preparedSnapshotRow struct {
	ID       int    `sqlx:"id,primaryKey"`
	Required *int   `sqlx:"required,required"`
	Value    string `sqlx:"value" validate:"datly_prepared_snapshot"`
}

func TestFrameworkPreparedDeferralSurvivesCallbacks(t *testing.T) {
	for _, fromSource := range []bool{true, false} {
		name := "predicate"
		if fromSource {
			name = "source"
		}
		t.Run(name, func(t *testing.T) {
			deferred := fields{"Value": true, "Required": true}
			calls := 0
			mutate := func() { delete(deferred, "Value"); delete(deferred, "Required"); deferred["ID"] = true }
			govalidator.Register("datly_prepared_snapshot", func(*govalidator.Field, *govalidator.Check) (govalidator.IsValid, error) {
				return func(context.Context, interface{}) (bool, error) {
					calls++
					if !fromSource {
						mutate()
					}
					return true, nil
				}, nil
			})
			h := sqlite.New(t)
			one := 1
			source := &callbackSource{db: h.DB}
			rows := []*preparedSnapshotRow{{ID: 1, Required: &one, Value: "first"}, {ID: 2, Value: "deferred"}}
			policies := []xhandler.ValidationOptions{{Action: xhandler.WriteInsert, Shallow: true}, {Action: xhandler.WriteInsert, Shallow: true, DeferredFields: deferred}}
			wantCalls := 1
			if fromSource {
				source.callback = mutate
				rows = rows[1:]
				policies = policies[1:]
				wantCalls = 0
			}
			result, err := validation.New(source).Validate(context.Background(), rows, policies)
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed || calls != wantCalls || source.calls != 1 || !deferred["ID"] {
				t.Fatalf("result=%v calls=%d want=%d source=%d mask=%v", result, calls, wantCalls, source.calls, deferred)
			}
		})
	}
}

func TestFrameworkPreparedCoverageSharedByGoAndSQL(t *testing.T) {
	coverage := fields{"Required": true}
	calls := 0
	govalidator.Register("datly_prepared_snapshot", func(*govalidator.Field, *govalidator.Check) (govalidator.IsValid, error) {
		return func(context.Context, interface{}) (bool, error) {
			calls++
			coverage["Required"] = false
			coverage["Value"] = true
			return true, nil
		}, nil
	})
	h := sqlite.New(t)
	one := 1
	result, err := validation.New(&callbackSource{db: h.DB}).Validate(context.Background(), []*preparedSnapshotRow{{ID: 1, Required: &one}, {ID: 2}}, []xhandler.ValidationOptions{
		{Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[0]"},
		{Action: xhandler.WriteUpdate, Shallow: true, Location: "Rows[1]", Previous: &preparedSnapshotRow{ID: 2}, PreviousFields: fields{"ID": true}, Fields: coverage},
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !result.Failed || len(result.Violations) != 1 || result.Violations[0].Location != "Rows[1].Required" || result.Violations[0].Check != "notnull" || coverage["Required"] {
		t.Fatalf("mutable coverage altered validation: calls=%d result=%+v mask=%v", calls, result, coverage)
	}
}
