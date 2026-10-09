package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestGeneratedCubeExcludesTransientSelections(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	component := cubeFixture()
	component.RootView.Columns[1] = &spec.Column{Name: "FlightId", Source: "flight_id", Type: spec.TypeRef{Name: "int"}}
	component.RootView.Columns = append(component.RootView.Columns,
		&spec.Column{Name: "DaysRemaining", Source: "days_remaining", Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-" json:"daysRemaining"`},
		&spec.Column{Name: "ComputedDimension", Source: "computed_dimension", Type: spec.TypeRef{Name: "string"}, Groupable: component.RootView.Groupable, Tag: `sqlx:"-" json:"computedDimension"`},
	)
	// DQL can retain discovery placeholders for fields populated by hooks.
	component.RootView.Source.SQL = "SELECT account_id, MAX(flight_id) AS flight_id, 0 AS days_remaining, '' AS computed_dimension FROM spend GROUP BY account_id"
	before := component.Clone()
	directory := filepath.Join(root, "reporting")
	generator := New(Input{Component: component, TargetPackage: "example.com/generated/reporting"})
	generated, err := generator.Generate(directory)
	require.NoError(t, err)
	require.Equal(t, before, component.Clone())
	require.Len(t, generated.Plan.Cubes, 1)
	definition := generated.Plan.Cubes[0].Definition
	require.Len(t, definition.Dimensions, 1)
	require.Len(t, definition.Measures, 1)
	require.Equal(t, "Measures.FlightId", definition.Measures[0].Field)
	first, err := os.ReadFile(filepath.Join(directory, "spend_cube.go"))
	require.NoError(t, err)
	_, err = generator.Generate(directory)
	require.NoError(t, err)
	second, err := os.ReadFile(filepath.Join(directory, "spend_cube.go"))
	require.NoError(t, err)
	require.Equal(t, string(first), string(second))
	runtimeTest := strings.ReplaceAll(`package CUBE_PACKAGE
import("encoding/json";"reflect";"testing")
func TestTransientReaderFieldsAreNotLinkedCubeSelections(t *testing.T) {
 input:=reflect.TypeFor[SpendCubeInput]()
 for _,section:=range []string{"Dimensions","Measures"} {
  field,ok:=input.FieldByName(section)
  if !ok||field.Type.NumField()!=1 {t.Fatalf("unexpected %s fields: %v",section,field)}
  for _,name:=range []string{"DaysRemaining","ComputedDimension"} {
   if _,found:=field.Type.FieldByName(name);found {t.Fatalf("transient %s exposed in %s",name,section)}
  }
 }
 var selection SpendCubeInput
 if err:=json.Unmarshal([]byte("{\"measures\":{\"flightId\":true}}"),&selection);err!=nil {t.Fatal(err)}
 if !selection.Measures.FlightId {t.Fatal("aggregate measure lost")}
 if _,err:=NewSpendCube();err!=nil {t.Fatal(err)}
 row:=SpendRow{DaysRemaining:7,ComputedDimension:"computed"}
 typ:=reflect.TypeFor[SpendRow]()
 for _,name:=range []string{"DaysRemaining","ComputedDimension"} {
  field,ok:=typ.FieldByName(name)
  if !ok||field.Tag.Get("sqlx")!="-" {t.Fatalf("reader field changed: %s",name)}
 }
 encoded,err:=json.Marshal(row);if err!=nil {t.Fatal(err)}
 var output map[string]any
 if err=json.Unmarshal(encoded,&output);err!=nil {t.Fatal(err)}
 if output["daysRemaining"]!=float64(7)||output["computedDimension"]!="computed" {t.Fatalf("computed reader output changed: %s",encoded)}
}
`, "CUBE_PACKAGE", generated.Plan.PackageName())
	require.NoError(t, os.WriteFile(filepath.Join(directory, "transient_test.go"), []byte(runtimeTest), 0600))
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
