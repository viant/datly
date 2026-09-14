package validation_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/validation"
	xhandler "github.com/viant/xdatly/handler"
)

type goPolicyRow struct {
	ID      int    `sqlx:"id,primaryKey"`
	Name    string `sqlx:"name" validate:"required"`
	Code    int    `sqlx:"code" validate:"required"`
	Enabled *bool  `sqlx:"enabled" validate:"notnull"`
}

func TestFrameworkGoPolicyCoverage(t *testing.T) {
	h := sqlite.New(t)
	validator := dml.NewData(h.DB).FrameworkValidator()
	f := false
	for _, tc := range []struct {
		name   string
		value  *goPolicyRow
		policy xhandler.ValidationOptions
		want   []string
	}{
		{"insert full", &goPolicyRow{Name: "ready", Code: 1, Enabled: &f}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, nil},
		{"update selected name", &goPolicyRow{ID: 1}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Previous: &goPolicyRow{ID: 1}, PreviousFields: fields{"ID": true}, Fields: fields{"Name": true}, Shallow: true}, []string{"Name"}},
		{"update omitted rules", &goPolicyRow{ID: 1}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Previous: &goPolicyRow{ID: 1}, PreviousFields: fields{"ID": true}, Fields: fields{}, Shallow: true}, nil},
		{"insert all missing", &goPolicyRow{}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, []string{"Name", "Code", "Enabled"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := validator.Validate(context.Background(), tc.value, tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, v := range result.Violations {
				got = append(got, v.Field)
			}
			if !reflect.DeepEqual(got, tc.want) || result.Failed != (len(tc.want) > 0) {
				t.Fatalf("fields=%v want=%v result=%+v", got, tc.want, result)
			}
		})
	}
}

type PromotedGoPolicy struct {
	Name    string `sqlx:"name" validate:"required"`
	Enabled *bool  `sqlx:"enabled" validate:"notnull"`
}
type promotedGoPolicyRow struct {
	ID int `sqlx:"id,primaryKey"`
	*PromotedGoPolicy
}

func TestFrameworkPromotedGoPolicyCoverage(t *testing.T) {
	h := sqlite.New(t)
	validator := dml.NewData(h.DB).FrameworkValidator()
	f := false
	for _, tc := range []struct {
		name   string
		value  *promotedGoPolicyRow
		policy xhandler.ValidationOptions
		want   int
	}{
		{"nil holder full", &promotedGoPolicyRow{}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, 2},
		{"present false", &promotedGoPolicyRow{PromotedGoPolicy: &PromotedGoPolicy{Name: "ready", Enabled: &f}}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, 0},
		{"nil holder sparse", &promotedGoPolicyRow{ID: 1}, xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, Previous: &promotedGoPolicyRow{ID: 1}, PreviousFields: fields{"ID": true}, Fields: fields{"Name": true}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holder := tc.value.PromotedGoPolicy
			result, err := validator.Validate(context.Background(), tc.value, tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Violations) != tc.want || result.Failed != (tc.want > 0) {
				t.Fatalf("got %+v want %d violations", result, tc.want)
			}
			for _, v := range result.Violations {
				if v.Field != "Name" && v.Field != "Enabled" {
					t.Fatalf("lost promoted field: %+v", v)
				}
			}
			if tc.value.PromotedGoPolicy != holder {
				t.Fatal("validation allocated or replaced the embedded holder")
			}
		})
	}
}

type goSlicePolicyRow struct {
	ID     int    `sqlx:"id,primaryKey"`
	Values []int  `sqlx:"-" validate:"between(1,3)"`
	Child  *goRow `sqlx:"-"`
}

func TestFrameworkShallowPrimitiveSliceRules(t *testing.T) {
	h := sqlite.New(t)
	validator := dml.NewData(h.DB).FrameworkValidator()
	for _, tc := range []struct {
		values []int
		want   int
	}{{[]int{1, 2, 3}, 0}, {[]int{1, 4}, 1}} {
		result, err := validator.Validate(context.Background(), &goSlicePolicyRow{Values: tc.values, Child: &goRow{}}, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, Location: "Rows[2]"})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Violations) != tc.want {
			t.Fatalf("got %+v want %d violations", result, tc.want)
		}
		if tc.want > 0 && result.Violations[0].Location != "Rows[2].Values[1]" {
			t.Fatalf("wrong element location: %+v", result.Violations[0])
		}
	}
}

func TestFrameworkUnknownGoRulePreflight(t *testing.T) {
	source := &noConnection{}
	value := &struct {
		ID   int    `sqlx:"id,primaryKey"`
		Name string `sqlx:"name" validate:"unknown_datly_rule"`
	}{}
	_, err := validation.New(source).Validate(context.Background(), value, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
	if err == nil || !strings.Contains(err.Error(), "unknown check") || source.calls != 0 {
		t.Fatalf("error=%v connection calls=%d", err, source.calls)
	}
}

func TestFrameworkBatchIndependentGoCoverage(t *testing.T) {
	h := sqlite.New(t)
	policies := []xhandler.ValidationOptions{
		{Action: xhandler.WriteUpdate, Shallow: true, Previous: &goPolicyRow{ID: 1}, PreviousFields: fields{"ID": true}, Fields: fields{"Name": true}, Location: "Names[0]"},
		{Action: xhandler.WriteUpdate, Shallow: true, Previous: &goPolicyRow{ID: 2}, PreviousFields: fields{"ID": true}, Fields: fields{"Enabled": true}, Location: "Flags[3]"},
		{Action: xhandler.WriteUpdate, Shallow: true, Previous: &goPolicyRow{ID: 3}, PreviousFields: fields{"ID": true}, Fields: fields{}, Location: "Omitted[4]"},
	}
	result, err := dml.NewData(h.DB).FrameworkValidator().Validate(context.Background(), []*goPolicyRow{{ID: 1}, {ID: 2}, {ID: 3}}, policies)
	if err != nil {
		t.Fatal(err)
	}
	locations := map[string]string{}
	for _, v := range result.Violations {
		locations[v.Location] = v.Check
	}
	if !result.Failed || len(result.Violations) != 2 || !reflect.DeepEqual(locations, map[string]string{"Names[0].Name": "required", "Flags[3].Enabled": "notnull"}) {
		t.Fatalf("candidate coverage leaked: %+v", result)
	}
}
