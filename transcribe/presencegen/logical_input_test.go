package presencegen

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecursivePresenceExcludesLogicalInputs(t *testing.T) {
	for _, relationTag := range []string{`sqlx:"-"`, `diff:"-" on:"ID=ROOT_ID" view:"children"`, `sqlx:"-" diff:"-" on:"ID=ROOT_ID" view:"children"`} {
		t.Run(relationTag, func(t *testing.T) {
			source := strings.ReplaceAll(logicalInputShape, "RELATION_TAG", relationTag)
			generated, err := Generate("fixture", map[string][]byte{"shape.go": []byte(source)}, []string{"Root"}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			for name, body := range map[string][]byte{
				"go.mod":   []byte("module fixture\n\ngo 1.23\n"),
				"shape.go": []byte(source), "presence.go": generated,
				"presence_test.go": []byte(logicalInputChecks),
			} {
				if err := os.WriteFile(filepath.Join(dir, name), body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("go", "test", "-count=1", ".")
			command.Dir = dir
			command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated comparison: %v\n%s", err, output)
			}
		})
	}
}

const logicalInputShape = `package fixture
type Root struct {
 ID int ` + "`sqlx:\"ID,primaryKey\"`" + `
 Input []*LogicalInput ` + "`sqlx:\"-\" diff:\"-\"`" + `
 Children []*Child ` + "`RELATION_TAG`" + `
 Has *RootHas ` + "`setMarker:\"true\"`" + `
}
type RootHas struct { ID, Input, Children bool }
type LogicalInput struct { Value int; Has *LogicalHas ` + "`setMarker:\"true\"`" + ` }
type LogicalHas struct { Value bool }
type Child struct {
 ID int ` + "`sqlx:\"ID,primaryKey\"`" + `
 Value int ` + "`sqlx:\"VALUE\"`" + `
 Has *ChildHas ` + "`setMarker:\"true\"`" + `
}
type ChildHas struct { ID, Value bool }
`

const logicalInputChecks = `package fixture
import "testing"
func TestPresence(t *testing.T) {
 for _, changed := range []bool{false, true} {
  input := &LogicalInput{Value: 9, Has: &LogicalHas{Value: true}}
  previous := &Root{ID: 1, Children: []*Child{{ID: 2, Value: 3}}}
  value := 3
  if changed { value++ }
  row := &Root{ID: 1, Input: []*LogicalInput{input}, Children: []*Child{{ID: 2, Value: value, Has: &ChildHas{ID: true, Value: true}}}, Has: &RootHas{ID: true, Input: true, Children: true}}
  row.SynchronizePresentMarker(previous)
  if row.Has == nil || !row.Has.ID || row.Has.Input || row.Has.Children != changed { t.Fatalf("root presence: %+v", row.Has) }
  if !input.Has.Value || input.Value != 9 { t.Fatal("logical input mutated") }
  if row.Children[0].Has.Value != changed { t.Fatal("physical child comparison skipped") }
 }
}
`
