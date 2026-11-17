package writer

import (
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

type immutableTimeRow struct {
	At     time.Time
	hidden time.Time
	Nested map[string]*immutableTimeChild
}

type immutableTimeChild struct{ Value int }

func immutableTimeSnapshot(t *testing.T, value any) string {
	t.Helper()
	snapshot, err := immutableValues([]reflect.Value{reflect.ValueOf(value)})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// Local's timezone data is lazy process state. Reading the zone through the
// standard library does not mutate the time.Time, its location pointer, or any
// writer body. A subprocess ensures the before/after cases do not initialize
// each other's cache, without replacing time.Local or resetting a sync.Once.
func TestImmutableValuesTimeLocationCache(t *testing.T) {
	for _, initializeBefore := range []bool{false, true} {
		name := "initialize after capture"
		if initializeBefore {
			name = "initialize before capture"
		}
		t.Run(name, func(t *testing.T) {
			const childKey = "DATLY_IMMUTABLE_TIME_TEST_CHILD"
			if os.Getenv(childKey) != t.Name() {
				pattern := "^" + strings.ReplaceAll(regexp.QuoteMeta(t.Name()), "/", "$/^") + "$"
				command := exec.CommandContext(t.Context(), os.Args[0], "-test.run="+pattern, "-test.count=1")
				command.Env = append(os.Environ(), childKey+"="+t.Name())
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("isolated native immutable-value guard: %v\n%s", err, output)
				}
				return
			}
			if initializeBefore {
				_ = time.Local.String()
			}
			row := &immutableTimeRow{At: time.Now(), Nested: map[string]*immutableTimeChild{"child": {Value: 1}}}
			row.hidden = row.At
			original := row.At
			before := immutableTimeSnapshot(t, row)
			_ = time.Local.String()
			if row.At != original || row.hidden != original || row.At.Location() != original.Location() {
				t.Fatal("fixture changed the timestamp or its location identity")
			}
			after := immutableTimeSnapshot(t, row)
			if before != after {
				t.Fatal("standard-library timezone initialization was mistaken for a queued value mutation")
			}
		})
	}
}

func TestImmutableValuesTimeMutationControls(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*immutableTimeRow)
	}{
		{"timestamp value", func(row *immutableTimeRow) { row.At = row.At.Add(time.Nanosecond) }},
		{"unexported timestamp value", func(row *immutableTimeRow) { row.hidden = row.hidden.Add(time.Nanosecond) }},
		{"monotonic reading", func(row *immutableTimeRow) { row.At = row.At.Round(0) }},
		{"location identity at equal instant", func(row *immutableTimeRow) {
			original := row.At
			row.At = row.At.In(time.FixedZone("replacement", 0))
			if !row.At.Equal(original) || row.At.Location() == original.Location() {
				t.Fatal("fixture must replace only the location at the same instant")
			}
		}},
		{"nested data value", func(row *immutableTimeRow) { row.Nested["child"].Value++ }},
		{"nested association identity", func(row *immutableTimeRow) {
			row.Nested["child"] = &immutableTimeChild{Value: row.Nested["child"].Value}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &immutableTimeRow{At: time.Date(2026, 11, 17, 1, 2, 3, 4, time.UTC), Nested: map[string]*immutableTimeChild{"child": {Value: 1}}}
			row.hidden = row.At
			if tc.name == "monotonic reading" {
				row.At = time.Now()
				if row.At == row.At.Round(0) {
					t.Fatal("fixture requires time.Now's monotonic reading")
				}
			}
			before := immutableTimeSnapshot(t, row)
			tc.mutate(row)
			if after := immutableTimeSnapshot(t, row); before == after {
				t.Fatal("actual queued value or association mutation was not detected")
			}
		})
	}
}
