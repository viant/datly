package validation_test

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
)

func TestFrameworkValidationOptionsFailClosed(t *testing.T) {
	valid := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}
	for _, tc := range []struct {
		name    string
		value   any
		options []any
		want    string
	}{
		{"no policy", &row{}, nil, "exactly one"},
		{"unknown option", &row{}, []any{"bad"}, "unsupported"},
		{"nil entity", (*row)(nil), []any{valid}, "entity is required"},
		{"collection not entity", []*row{}, []any{valid}, "one typed entity"},
		{"unknown action", &row{}, []any{xhandler.ValidationOptions{Action: "delete", Shallow: true}}, "unsupported"},
		{"deep request", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteInsert}}, "shallow"},
		{"insert not full", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, Fields: fields{}}}, "full coverage"},
		{"update no read provenance", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, Previous: &row{}, Fields: fields{}}}, "read provenance"},
		{"wrong previous type", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, Previous: &goRow{}, PreviousFields: fields{}, Fields: fields{}}}, "does not match"},
		{"producer deferral on update", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, DeferredFields: fields{"ID": true}}}, "require an insert"},
		{"deferral with satisfied references", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, DeferredFields: fields{"ID": true}, SatisfiedReferences: []xhandler.ValidationReference{{Field: "ID", Table: "parents", Column: "id"}}}}, "cannot accompany"},
		{"planned references on update", &row{}, []any{xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, SatisfiedReferences: []xhandler.ValidationReference{{Field: "ID", Table: "parents", Column: "id"}}}}, "require an insert"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			_, err := dml.NewData(h.DB).FrameworkValidator().Validate(context.Background(), tc.value, tc.options...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v want %s", err, tc.want)
			}
		})
	}
}

type GoRules struct {
	Name string `validate:"required"`
}
type promotedRules struct{ *GoRules }
type nullRule struct {
	Value *bool `validate:"notnull"`
}

func TestFrameworkNativePromotedAndNullRules(t *testing.T) {
	for _, tc := range []struct {
		name  string
		row   any
		check string
	}{
		{"promoted rule", &promotedRules{}, "required"},
		{"notnull", &nullRule{}, "notnull"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			result, err := dml.NewData(h.DB).FrameworkValidator().Validate(context.Background(), tc.row, xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true})
			if err != nil {
				t.Fatal(err)
			}
			if !result.Failed || len(result.Violations) != 1 || result.Violations[0].Check != tc.check {
				t.Fatalf("got %+v want %s", result, tc.check)
			}
		})
	}
}
