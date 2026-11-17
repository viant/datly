package presencegen

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Session attributes use the standard named byte slice, including under an
// import alias. Compile and execute the generated comparison, not just its text.
func TestGeneratedRawMessageEquality(t *testing.T) {
	for _, rowOnly := range []bool{false, true} {
		for _, alias := range []string{"json", "wire"} {
			t.Run(fmt.Sprintf("rowOnly=%v/import=%s", rowOnly, alias), func(t *testing.T) {
				source := strings.ReplaceAll(rawMessageShape, "JSON", alias)
				generated, err := Generate("fixture", map[string][]byte{"shape.go": []byte(source)}, []string{"Row"}, Options{RowOnly: rowOnly})
				if err != nil {
					t.Fatal(err)
				}
				dir := t.TempDir()
				for name, content := range map[string][]byte{
					"go.mod":           []byte("module fixture\n\ngo 1.23\n"),
					"shape.go":         []byte(source),
					"presence.go":      generated,
					"presence_test.go": []byte(rawMessageChecks),
				} {
					if err := os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
						t.Fatal(err)
					}
				}
				command := exec.Command("go", "test", "-count=1", ".")
				command.Dir = dir
				command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("generated comparison failed: %v\n%s\n%s", err, output, generated)
				}
			})
		}
	}
}

const rawMessageShape = `package fixture
import JSON "encoding/json"
type Row struct {
 ID int ` + "`sqlx:\"ID,primaryKey\"`" + `
 Value JSON.RawMessage
 Optional *JSON.RawMessage
 Bytes []byte
 Has *RowHas ` + "`setMarker:\"true\"`" + `
}
type RowHas struct { ID, Value, Optional, Bytes bool }
`

const rawMessageChecks = `package fixture
import (
 "encoding/json"
 "reflect"
 "testing"
)
func TestRawPresence(t *testing.T) {
 for _, tc := range []struct {
  name string
  previous, current json.RawMessage
  present, changed bool
 }{
  {"equal separately allocated bytes", json.RawMessage("{\"a\":1}"), json.RawMessage("{\"a\":1}"), true, false},
  {"changed bytes", json.RawMessage("1"), json.RawMessage("2"), true, true},
  {"both nil", nil, nil, true, false},
  {"both empty", json.RawMessage{}, json.RawMessage{}, true, false},
  {"nil to empty", nil, json.RawMessage{}, true, true},
  {"empty to nil", json.RawMessage{}, nil, true, true},
  {"value to null", json.RawMessage("1"), nil, true, true},
  {"unmarked change", json.RawMessage("1"), json.RawMessage("2"), false, false},
 } {
  t.Run(tc.name, func(t *testing.T) {
   prev := &Row{ID:1, Value:tc.previous, Optional:&tc.previous, Bytes:tc.previous, Has:&RowHas{ID:true, Value:true, Optional:true, Bytes:true}}
   current := &Row{ID:1, Value:tc.current, Optional:&tc.current, Bytes:tc.current, Has:&RowHas{ID:true, Value:tc.present, Optional:tc.present, Bytes:tc.present}}
   oldMarker, oldPresence := current.Has, *current.Has
   prevMarker := *prev.Has
   current.SynchronizePresentMarker(prev)
   if current.Has == nil || !current.Has.ID { t.Fatal("identity marker lost") }
   if current.Has.Value != tc.changed || current.Has.Optional != tc.changed || current.Has.Bytes != tc.changed { t.Fatalf("presence=%+v, changed=%v",current.Has,tc.changed) }
   if *oldMarker != oldPresence || *prev.Has != prevMarker { t.Fatal("shared markers mutated") }
   if !reflect.DeepEqual(current.Value,tc.current) || !reflect.DeepEqual(prev.Value,tc.previous) { t.Fatal("values mutated") }
  })
 }
 value := json.RawMessage("1")
 for _, tc := range []struct { before, after *json.RawMessage; changed bool }{
  {nil,nil,false}, {nil,&value,true}, {&value,nil,true}, {&value,&value,false},
 } {
  row := &Row{Optional:tc.after, Has:&RowHas{Optional:true}}
  row.SynchronizePresentMarker(&Row{Optional:tc.before})
  if (row.Has != nil && row.Has.Optional) != tc.changed { t.Fatal("pointer presence mismatch") }
 }
 row := &Row{Value:value, Has:&RowHas{Value:true}}
 marker := row.Has
 row.SynchronizePresentMarker(nil)
 if row.Has != marker || !row.Has.Value { t.Fatal("nil Previous changed presence") }
}
`
