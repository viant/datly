package presencegen

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Execute the renderer's output: checking only generated strings would miss
// equality differences between Go struct identity and persisted instants.
func TestGeneratedTimeEquality(t *testing.T) {
	for _, rowOnly := range []bool{false, true} {
		for _, alias := range []string{"time", "clock"} {
			t.Run(fmt.Sprintf("rowOnly=%v/import=%s", rowOnly, alias), func(t *testing.T) {
				source := strings.ReplaceAll(timeEqualityShape, "CLOCK", alias)
				generated, err := Generate("fixture", map[string][]byte{"shape.go": []byte(source)}, []string{"Row"}, Options{RowOnly: rowOnly})
				if err != nil {
					t.Fatal(err)
				}
				dir := t.TempDir()
				for name, content := range map[string][]byte{
					"go.mod":           []byte("module fixture\n\ngo 1.23\n"),
					"shape.go":         []byte(source),
					"presence.go":      generated,
					"presence_test.go": []byte(timeEqualityChecks),
				} {
					if err := os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
						t.Fatal(err)
					}
				}
				command := exec.Command("go", "test", "-count=1", ".")
				command.Dir = dir
				command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("generated comparison failed: %v\n%s\n%s", err, output, generated)
				}
			})
		}
	}
}

const timeEqualityShape = `package fixture
import CLOCK "time"
type Row struct {
 ID int ` + "`sqlx:\"ID,primaryKey\"`" + `
 At CLOCK.Time ` + "`sqlx:\"AT\"`" + `
 Optional *CLOCK.Time ` + "`sqlx:\"OPTIONAL\"`" + `
 Has *RowHas ` + "`setMarker:\"true\"`" + `
}
type RowHas struct { ID, At, Optional bool }
`

const timeEqualityChecks = `package fixture
import (
 "testing"
 "time"
)
func TestTimePresence(t *testing.T) {
 now := time.Now()
 wall := now.Round(0)
 zone := wall.In(time.FixedZone("different", 7*60*60))
 later := wall.Add(time.Nanosecond)
 for _, test := range []struct {
  name string
  previous, current time.Time
  previousPointer, currentPointer *time.Time
  present, changed bool
 }{
  {"same zone-independent instant", wall, zone, &wall, &zone, true, false},
  {"monotonic stripped", now, wall, &now, &wall, true, false},
  {"changed instant", wall, later, &wall, &later, true, true},
  {"both nil", wall, wall, nil, nil, true, false},
  {"null to value", wall, wall, nil, &wall, true, true},
  {"value to null", wall, wall, &wall, nil, true, true},
  {"shared pointer", wall, wall, &wall, &wall, true, false},
  {"unmarked change", wall, later, &wall, &later, false, false},
 } {
  t.Run(test.name, func(t *testing.T) {
   previous := &Row{ID: 1, At: test.previous, Optional: test.previousPointer, Has: &RowHas{ID: true, At: true, Optional: true}}
   before := *previous
   previousMarker := *previous.Has
   var pointedBefore time.Time
   if previous.Optional != nil { pointedBefore = *previous.Optional }
   row := &Row{ID: 1, At: test.current, Optional: test.currentPointer, Has: &RowHas{ID: true, At: test.present, Optional: test.present}}
   originalMarker := row.Has
   originalPresence := *row.Has
   row.SynchronizePresentMarker(previous)
   if row.Has == nil { t.Fatal("identity presence lost") }
   if row.Has.At != (test.present && !test.previous.Equal(test.current)) { t.Errorf("At presence = %v", row.Has.At) }
   if row.Has.Optional != test.changed { t.Errorf("Optional presence = %v, want %v", row.Has.Optional, test.changed) }
   if !row.Has.ID { t.Error("identity marker lost") }
   if *previous != before || *previous.Has != previousMarker { t.Error("Previous mutated") }
   if previous.Optional != nil && *previous.Optional != pointedBefore { t.Error("Previous pointer value mutated") }
   if *originalMarker != originalPresence { t.Error("shared request marker mutated") }
   if row.At != test.current || row.Optional != test.currentPointer { t.Error("request values mutated") }
  })
 }
 t.Run("nil previous retains presence", func(t *testing.T) {
  row := &Row{At: wall, Has: &RowHas{At: true}}
  original := row.Has
  row.SynchronizePresentMarker(nil)
  if row.Has != original || !row.Has.At { t.Fatal("nil Previous changed presence") }
 })
}
`
