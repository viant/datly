package validation_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	"github.com/viant/govalidator"
	xhandler "github.com/viant/xdatly/handler"
)

type availabilityRow struct {
	ID           int    `sqlx:"id,primaryKey"`
	GoGenerated  *int   `sqlx:"-" validate:"notnull"`
	SQLGenerated *int   `sqlx:"generated,required"`
	Echo         int    `sqlx:"echo" validate:"required,eqfield(ID)"`
	Name         string `sqlx:"name,unique,uniqueDep=id,table=availability" validate:"required"`
}

func TestFrameworkAvailabilityPhasesSQLite(t *testing.T) {
	for _, batch := range []bool{false, true} {
		mode := "scalar"
		if batch {
			mode = "batch"
		}
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE availability(id INTEGER,generated INTEGER,echo INTEGER,name TEXT,UNIQUE(id,name))", "INSERT INTO availability VALUES(2,1,2,'taken')"); err != nil {
				t.Fatal(err)
			}
			validator := dml.NewData(h.DB).FrameworkValidator()
			one := 1
			for _, tc := range []struct {
				name     string
				row      *availabilityRow
				deferred fields
				want     map[string]string
			}{
				{"business dependent checks wait", &availabilityRow{Echo: 1, Name: "taken"}, fields{"ID": true, "GoGenerated": true, "SQLGenerated": true}, map[string]string{}},
				{"business independent checks run", &availabilityRow{}, fields{"ID": true, "GoGenerated": true, "SQLGenerated": true}, map[string]string{"Rows[3].Echo": "required", "Rows[3].Name": "required"}},
				{"final valid", &availabilityRow{ID: 1, GoGenerated: &one, SQLGenerated: &one, Echo: 1, Name: "fresh"}, nil, map[string]string{}},
				{"final complete checks", &availabilityRow{ID: 2, Echo: 1, Name: "taken"}, nil, map[string]string{"Rows[3].Echo": "eqfield", "Rows[3].GoGenerated": "notnull", "Rows[3].SQLGenerated": "notnull", "Rows[3].Name": "unique"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					policy := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[3]", DeferredFields: tc.deferred}
					var value any = tc.row
					var options any = policy
					if batch {
						value = []*availabilityRow{tc.row}
						options = []xhandler.ValidationOptions{policy}
					}
					result, err := validator.Validate(ctx, value, options)
					if err != nil {
						t.Fatal(err)
					}
					got := map[string]string{}
					for _, v := range result.Violations {
						got[v.Location] = v.Check
					}
					if !reflect.DeepEqual(got, tc.want) || len(result.Violations) != len(tc.want) || result.Failed != (len(tc.want) > 0) {
						t.Fatalf("checks=%v want=%v result=%+v", got, tc.want, result)
					}
				})
			}
		})
	}
}

type opaqueAvailabilityRow struct {
	ID    int    `sqlx:"id,primaryKey"`
	Value string `sqlx:"value" validate:"datly_availability_opaque"`
}

func TestFrameworkAvailabilityPreflightsWholeBatch(t *testing.T) {
	calls := 0
	govalidator.Register("datly_availability_opaque", func(*govalidator.Field, *govalidator.Check) (govalidator.IsValid, error) {
		return func(context.Context, interface{}) (bool, error) { calls++; return true, nil }, nil
	})
	source := &noConnection{}
	_, err := validation.New(source).Validate(context.Background(), []*opaqueAvailabilityRow{{ID: 1}, {ID: 2}}, []xhandler.ValidationOptions{
		{Action: xhandler.WriteInsert, Shallow: true},
		{Action: xhandler.WriteInsert, Shallow: true, DeferredFields: fields{"ID": true}},
	})
	if err == nil || calls != 0 || source.calls != 0 {
		t.Fatalf("late opaque policy: error=%v predicate calls=%d source calls=%d", err, calls, source.calls)
	}
	result, err := validation.New(source).Validate(context.Background(), &opaqueAvailabilityRow{}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
	if err != nil || result.Failed || calls != 1 {
		t.Fatalf("ordinary registration changed: result=%v error=%v calls=%d", result, err, calls)
	}
}
