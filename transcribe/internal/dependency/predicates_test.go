package dependency

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/dql"
)

func TestPredicateDependenciesUseEachDeclarationContext(t *testing.T) {
	for _, tc := range []struct {
		name, imports, reference, scope, want string
	}{
		{name: "first alias", imports: "#import('security', 'example.com/first')", reference: "security.Check", scope: "example.com/reader", want: "example.com/first"},
		{name: "second alias", imports: "#import('security', 'example.com/second')", reference: "security.Check", scope: "example.com/reader", want: "example.com/second"},
		{name: "local declaration", reference: "Check", scope: "example.com/reader", want: "example.com/reader"},
		{name: "absolute without scope", reference: "example.com/security.Check", want: "example.com/security"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			packages := map[string]bool{}
			require.NoError(t, collectPredicatePackages(packages, dql.PrepareSource(tc.imports+"\n#set($_ = $Minimum<int>(query/min).WithPredicate(0, 'handler', '"+tc.reference+"'))\nSELECT id FROM records"), tc.scope))
			require.Equal(t, map[string]bool{tc.want: true}, packages)
		})
	}
	packages := map[string]bool{}
	require.NoError(t, collectPredicatePackages(packages, dql.PrepareSource("SELECT 1"), ""))
	require.Empty(t, packages)
}
