package golang

import (
	"reflect"
	"testing"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestSequenceReservationFieldsUseDomainAuthority(t *testing.T) {
	owner := actionRole{record: &recordLowering{
		value: recordShape{base: "Parent"},
		plan: &plan.RecordPlan{Table: "records", Sequence: &plan.SequencePlan{
			Field: plan.FieldRef{Field: "ID", Source: "id"},
		}},
	}}
	emitter := &actionEmitter{roles: []actionRole{owner, owner}}
	for _, tc := range []struct {
		name, entity, table string
		keys                []plan.KeyPart
		want                []string
	}{
		{"same type without sequence", "Parent", "records", nil, []string{"ID"}},
		{"another type same physical key", "Child", "records", []plan.KeyPart{{Field: "Tenant", Source: "tenant_id"}, {Field: "RecordID", Source: "id"}}, []string{"RecordID"}},
		{"same type registers its own table authority", "Parent", "other", nil, []string{"ID"}},
		{"same type table alias", "Parent", "RECORDS", nil, []string{"ID"}},
		{"unrelated key source", "Child", "records", []plan.KeyPart{{Field: "ID", Source: "tenant_id"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role := actionRole{record: &recordLowering{value: recordShape{base: tc.entity}, plan: &plan.RecordPlan{Table: tc.table, Keys: tc.keys}}}
			if got := emitter.sequenceFields(role); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("reservation fields=%v want=%v", got, tc.want)
			}
		})
	}
}
