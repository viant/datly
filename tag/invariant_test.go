package tag

import (
	"reflect"
	"testing"
)

func TestParseInvariant(t *testing.T) {
	for _, test := range []struct {
		value, want string
		invalid     bool
	}{
		{"Schedule", "Schedule", false}, {"work_schedule", "work_schedule", false},
		{" A ", "A", false}, {"", "", true}, {"A,B", "", true},
		{"A B", "", true}, {"A|B", "", true}, {"A;B", "", true},
	} {
		t.Run(test.value, func(t *testing.T) {
			actual, err := ParseInvariant(test.value)
			if (err != nil) != test.invalid || actual != test.want {
				t.Fatalf("ParseInvariant(%q) = %q, %v", test.value, actual, err)
			}
		})
	}
	field, _ := reflect.TypeOf(struct {
		Start string `invariant:"Schedule"`
	}{}).FieldByName("Start")
	metadata, err := ParseField(field)
	if err != nil || metadata.Invariant != "Schedule" {
		t.Fatalf("field invariant: %+v, %v", metadata, err)
	}
}
