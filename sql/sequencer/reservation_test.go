package sequencer

import (
	"math"
	"testing"

	"github.com/viant/sqlx/metadata/sink"
)

func TestReservationUsesNativeIdentityAndRangeArithmetic(t *testing.T) {
	service := New(nil)
	for _, tc := range []struct {
		table, name, schema string
		value               int64
		count               int
		want                int64
	}{
		{"first spelling", "shared_sequence", "main", 3, 2, 1},
		{"second spelling", "shared_sequence", "main", 3, 2, 3},
		{"first spelling", "other_sequence", "main", 2, 1, 1},
		{"first spelling", "shared_sequence", "other", 2, 1, 1},
		{"first spelling", "shared_sequence", "main", 103, 2, 101},
		{"first spelling", "shared_sequence", "main", 3, 2, 103},
	} {
		native := &sink.Sequence{Catalog: "catalog", Schema: tc.schema, Name: tc.name, StartValue: 1, IncrementBy: 1, Value: tc.value}
		result, err := service.reserve(tc.table, native, tc.count)
		if err != nil || result.MinValue(int64(tc.count)) != tc.want {
			t.Fatalf("%s/%s: sequence=%+v err=%v want=%d", tc.schema, tc.name, result, err, tc.want)
		}
		if native.Value != tc.value {
			t.Fatal("native range metadata was mutated")
		}
	}
	independent, err := New(nil).reserve("first spelling", &sink.Sequence{Name: "shared_sequence", StartValue: 1, IncrementBy: 1, Value: 2}, 1)
	if err != nil || independent.MinValue(1) != 1 {
		t.Fatal("reservation escaped its DB/transaction service owner")
	}
}

func TestReservationRejectsInvalidChangedAndOverflowingRanges(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sequence *sink.Sequence
		count    int
	}{
		{"nil", nil, 1},
		{"zero count", &sink.Sequence{StartValue: 1, IncrementBy: 1, Value: 2}, 0},
		{"zero increment", &sink.Sequence{StartValue: 1, Value: 2}, 1},
		{"width overflow", &sink.Sequence{StartValue: 1, IncrementBy: math.MaxInt64, Value: 2}, 2},
	} {
		if _, err := New(nil).reserve(tc.name, tc.sequence, tc.count); err == nil {
			t.Fatalf("accepted %s", tc.name)
		}
	}
	service := New(nil)
	if _, err := service.reserve("records", &sink.Sequence{Name: "records", StartValue: 1, IncrementBy: 1, Value: math.MaxInt64}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := service.reserve("records", &sink.Sequence{Name: "records", StartValue: 1, IncrementBy: 1, Value: 2}, 1); err == nil {
		t.Fatal("accepted overflowing high-water range")
	}
	if _, err := service.reserve("records", &sink.Sequence{Name: "records", StartValue: 1, IncrementBy: 2, Value: 3}, 1); err == nil {
		t.Fatal("accepted changed increment")
	}
}
