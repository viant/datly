package validation_test

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/sql/dml"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

type presenceInsertRow struct {
	ID    int  `sqlx:"id,primaryKey"`
	Value *int `sqlx:"value,required" validate:"required"`
}

func TestInsertPresenceInitialAndCompleteFinalValidation(t *testing.T) {
	h := sqlite.New(t)
	if _, err := h.DB.Exec("CREATE TABLE rows(id INTEGER PRIMARY KEY,value INTEGER NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	service := dml.NewData(h.DB).FrameworkValidator()
	for _, tc := range []struct {
		name   string
		policy xhandler.ValidationOptions
		failed bool
	}{
		{"default remains complete", xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, true},
		{"omitted initial", xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, HonorPresence: true, Fields: fields{}}, false},
		{"supplied null initial", xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, HonorPresence: true, Fields: fields{"Value": true}}, true},
		{"complete final", xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := service.Validate(context.Background(), &presenceInsertRow{}, tc.policy)
			if err != nil {
				t.Fatal(err)
			}
			if result.Failed != tc.failed {
				t.Fatalf("got %+v", result)
			}
		})
	}
}
