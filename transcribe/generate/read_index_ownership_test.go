package generate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadIndexOwnershipRetainsOnlyProvenCache(t *testing.T) {
	dir := t.TempDir()
	source := `package records
 type RecordsReadIndexes struct { Value int }
 func (input *Input) PrepareReadIndexes() error { input._indexes = &RecordsReadIndexes{}; return nil }
 func (input *Input) ReadIndexes() *RecordsReadIndexes { return input._indexes }
 `
	file, err := parser.ParseFile(token.NewFileSet(), "proposal.go", source, 0)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "input.go"), []byte("package records; type Input struct { _indexes *RecordsReadIndexes }"), 0600))
	for _, tc := range []struct {
		name, owner, extra string
		want               bool
	}{
		{"owned", "Records", "", true},
		{"unowned", "", "", false},
		{"foreign", "Other", "", false},
		{"authored receiver", "Records", "func (*Input) Authored() {}", false},
		{"linked type", "Records", "type Input struct{}", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			header := ""
			if tc.owner != "" {
				header = generatedHeader(tc.owner)
			}
			content := header + source + tc.extra
			require.NoError(t, os.WriteFile(filepath.Join(dir, "selected.go"), []byte(content), 0600))
			for range 3 {
				require.Equal(t, tc.want, RetainsReadIndexCache(dir, "selected.go", "input.go", "Records", "Input", "_indexes", "RecordsReadIndexes", file))
			}
			p := &scaffoldPersistence{owner: "Records", plan: &Plan{Package: "example.com/records", GoPackage: "records", Input: ContractPlan{Type: "Input", Package: "example.com/records", Ownership: ContractLinked}, ReadIndexes: &ReadIndexSource{CacheField: "_indexes", Source: MutationSource{Destination: "selected.go", File: file}}}}
			require.Equal(t, tc.want, p.ownedReadIndexDestination("selected.go", []byte(content)))
			require.False(t, p.ownedReadIndexDestination("other.go", []byte(content)))
		})
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "selected.go"), []byte(generatedHeader("Records")+source), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "input.go"), []byte("package records; type Input struct { _indexes interface{} }"), 0600))
	require.False(t, RetainsReadIndexCache(dir, "selected.go", "input.go", "Records", "Input", "_indexes", "RecordsReadIndexes", file))
	// The native method bodies, not their names, establish generated support.
	changed := *file
	changed.Decls = append([]ast.Decl(nil), file.Decls...)
	method := *changed.Decls[len(changed.Decls)-1].(*ast.FuncDecl)
	method.Body = &ast.BlockStmt{}
	changed.Decls[len(changed.Decls)-1] = &method
	require.False(t, readIndexMethodsMatch([]byte(source), &changed, "Input"))
}
