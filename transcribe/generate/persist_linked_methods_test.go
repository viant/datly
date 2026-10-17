package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLinkedMethodCompanionSurvivesRegeneration(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{Package: "example.com/records", ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go", Field{Name: "Id", Type: "int", Tag: `parameter:"id,kind=query,in=id"`}), Output: generatedContract("Output", "output.go")}
	_, err := EmitScaffold(dir, plan)
	require.NoError(t, err)
	before := map[string][]byte{}
	for _, name := range []string{"input.go", "input_setters.go"} {
		before[name], err = os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
	}
	// A method-only artifact on an unrelated receiver is still obsolete.
	obsolete := generatedHeader("Records") + "package records\ntype Other struct{}\nfunc (*Other) SetId(int) {}\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "obsolete.go"), []byte(obsolete), 0600))
	plan.Input.Ownership = ContractLinked
	plan.Input.Package = plan.Package
	for range 2 {
		_, err = EmitScaffold(dir, plan)
		require.NoError(t, err)
		for name, want := range before {
			got, err := os.ReadFile(filepath.Join(dir, name))
			require.NoError(t, err)
			require.Equal(t, want, got, name)
		}
	}
	_, err = os.Stat(filepath.Join(dir, "obsolete.go"))
	require.True(t, os.IsNotExist(err))
	consumer := `package records
import "testing"
func TestConsumer(t *testing.T){input:=&Input{};input.SetId(7);if input.Id!=7{t.Fatal(input.Id)}}
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "consumer_test.go"), []byte(consumer), 0600))
	command := exec.Command("go", "test", "input.go", "input_setters.go", "consumer_test.go")
	command.Dir = dir
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	// A new generated destination cannot overwrite authoritative linked methods.
	plan.Output.Destination = "input_setters.go"
	_, err = EmitScaffold(dir, plan)
	require.ErrorContains(t, err, "contains a linked Go type")
	got, err := os.ReadFile(filepath.Join(dir, "input_setters.go"))
	require.NoError(t, err)
	require.Equal(t, before["input_setters.go"], got)
}

func TestLinkedSourceRecognizesOnlyExactLocalDeclarations(t *testing.T) {
	plan := &Plan{Package: "example.com/records", ComponentName: "Records", Input: ContractPlan{Type: "Input", Package: "example.com/records", Ownership: ContractLinked}, Output: ContractPlan{Type: "Output", Package: "example.com/records", Ownership: ContractLinked}, Views: []ViewPlan{{Type: "Row", Package: "example.com/records", Ownership: ViewLinked}, {Type: "Foreign", Package: "example.com/foreign", Ownership: ViewLinked}, {Type: "Generated", Package: "example.com/records", Ownership: ViewGenerated}}}
	persistence := &scaffoldPersistence{plan: plan}
	for _, tc := range []struct {
		name, source string
		want         bool
	}{
		{"input pointer", "package records;func (*Input) Existing(){}", true},
		{"input value", "package records;func (Input) Existing(){}", true},
		{"output pointer", "package records;func (*Output) Existing(){}", true},
		{"view value", "package records;func (Row) Existing(){}", true},
		{"linked type", "package records;type Input struct{}", true},
		{"other package", "package other;func (*Input) Existing(){}", false},
		{"type other package", "package other;type Input struct{}", false},
		{"unrelated receiver", "package records;func (*Other) SetInput(){}", false},
		{"foreign linked view", "package records;func (*Foreign) Existing(){}", false},
		{"generated view", "package records;func (*Generated) Existing(){}", false},
		{"qualified receiver", "package records;func (*foreign.Input) Existing(){}", false},
		{"generic receiver", "package records;func (*Input[T]) Existing(){}", false},
		{"free function", "package records;func SetInput(*Input) *Output{return nil}", false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, persistence.linkedSource([]byte(tc.source))) })
	}
}
