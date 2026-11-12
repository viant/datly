package transcribe

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/report"
)

func TestPredicateIgnoreTagNativeArtifact(t *testing.T) {
	const source = `#package('example.com/reader/component')
#setting($_ = $route('/reader','GET'))
#set($_ = $Value<string>(form/value).Optional().__DECLARATION__)
#set($_ = $Data<?>(output/view))
SELECT r.* FROM (SELECT 1 AS id) r
`
	for _, test := range []struct {
		name, declaration string
		fails             bool
	}{
		{"ignored tag", `WithTag('predicate:"-"')`, false},
		{"unknown tag", `WithTag('predicate:"missing_predicate"')`, true},
		{"explicit dash predicate", `WithPredicate(0,'-')`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			contract, err := NewCompiler().RuntimeContractsInModule(t.Context(), "example.com/reader", &Source{Name: "reader", Text: strings.ReplaceAll(source, "__DECLARATION__", test.declaration)})
			require.NoError(t, err)
			_, err = report.NewProjectCompiler(report.ProjectConfig{Types: contract.Types}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: contract.Component, InputType: contract.InputType, OutputType: contract.OutputType, Types: contract.Types, Resources: contract.Resources}})
			if test.fails {
				require.ErrorContains(t, err, "is not registered")
			} else {
				require.NoError(t, err)
				require.Empty(t, contract.Component.Parameters[0].Predicates)
			}
		})
	}
}
