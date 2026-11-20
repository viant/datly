package dml

import (
	"database/sql"
	"github.com/viant/datly/internal/associationtag"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/info/dialect"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

func TestAssociationInsertSingletonJournal(t *testing.T) {
	type link struct{ Key int }
	for _, tc := range []struct {
		product  string
		strategy dialect.PresetIDStrategy
		want     bool
	}{
		{"MySQL", "", true},
		{"MySQL", dialect.PresetIDStrategyUndefined, true},
		{"MySQL", dialect.PresetIDWithTransientTransaction, true},
		{"MySQL", dialect.PresetIDWithReservation, false},
		{"SQLite", dialect.PresetIDWithTransientTransaction, false},
	} {
		t.Run(tc.product+"/"+string(tc.strategy), func(t *testing.T) {
			d := NewData(new(sql.DB), WithSequenceStrategy(tc.strategy))
			d.dialect = &info.Dialect{Product: database.Product{Name: tc.product}, DefaultPresetIDStrategy: dialect.PresetIDWithTransientTransaction}
			row := &link{}
			supported, err := d.SupportsAssociationInsertIdentity(t.Context())
			if err != nil || supported != tc.want {
				t.Fatalf("supported %v: %v", supported, err)
			}
			if err := d.Delete("links", &link{Key: 2}); err != nil {
				t.Fatal(err)
			}
			if err := d.InsertAssociationWithIdentity(t.Context(), "links", row, &row.Key); (err == nil) != tc.want {
				t.Fatalf("admission: %v", err)
			}
			if err := d.Insert("events", &link{Key: 3}); err != nil {
				t.Fatal(err)
			}
			selected := d.operations("links", d)
			if !tc.want {
				if len(selected) != 1 {
					t.Fatalf("unsupported source admitted a write: %v", selected)
				}
				return
			}
			if len(selected) != 2 || selected[0].kind != dataOpDelete || selected[1].generatedKey != &row.Key || selected[1].data != row {
				t.Fatalf("lost prefix order or row ownership: %v", selected)
			}
			plan := buildExecutionPlan(append(selected, &dataOperation{table: "links", kind: dataOpInsert, data: &link{}}))
			if len(plan) != 3 || !plan[1].appendBarrier {
				t.Fatalf("generated identity shared an execution step: %+v", plan)
			}
		})
	}
}

type foreignAssociationValidator struct{ xhandler.Validator }

func TestAssociationIdentityValidatorOwnership(t *testing.T) {
	data := NewData(new(sql.DB))
	other := NewData(new(sql.DB))
	child := data.ComponentData(ComponentImperative, "1").(*Data)
	permit := associationtag.NativeIdentity()
	for _, tc := range []struct {
		label string
		v     xhandler.Validator
		want  bool
	}{
		{"exact component", data.FrameworkValidator(), true},
		{"other invocation", other.FrameworkValidator(), false},
		{"sibling frame", child.FrameworkValidator(), false},
		{"substituted binder", foreignAssociationValidator{data.FrameworkValidator()}, false},
		{"missing", nil, false},
	} {
		if got := data.OwnsAssociationValidator(permit, tc.v); got != tc.want {
			t.Fatalf("%s: %t", tc.label, got)
		}
	}
}
