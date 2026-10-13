package transcribe

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/program"
)

func TestGeneratedFactoryQualifiedOwnershipSelection(t *testing.T) {
	const destination = "example.com/app/records"
	for _, tc := range []struct {
		name, input string
		imports     []spec.ImportSpec
		generated   bool
	}{
		{name: "alias", input: "rows.Input", generated: true},
		{name: "full path", input: destination + ".Input", generated: true},
		{name: "compatible bare", input: "Input", generated: true},
		{name: "unknown alias", input: "unknown.Input"},
		{name: "foreign", input: "example.com/other.Input"},
		{name: "pointer envelope", input: "*rows.Input"},
		{name: "slice envelope", input: "[]rows.Input"},
		{name: "generic envelope", input: "rows.Input[string]"},
		{name: "private envelope", input: "rows.input"},
		{name: "conflicting alias", input: "rows.Input", imports: []spec.ImportSpec{{Alias: "rows", Package: "example.com/other"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			imports := append([]spec.ImportSpec{{Alias: "rows", Package: destination}}, tc.imports...)
			prepared := &dql.PreparedSource{TypeContext: &spec.TypeContext{PackagePath: destination, Imports: imports}}
			header := &dql.HandlerHeader{Declarative: true, Method: "PATCH", Factory: "rows.NewSave", InputType: tc.input, OutputType: destination + ".Output"}
			names, generated := program.FactoryContracts(header, prepared)
			require.Equal(t, tc.generated, generated)
			if generated {
				require.Equal(t, [3]string{"NewSave", "Input", "Output"}, names)
			}
		})
	}
}
