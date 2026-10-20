package transcribe

import (
	"bytes"
	gen "github.com/viant/datly/transcribe/generate"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/gobuild"
)

const borrowedAuthoredSetter = `package generated
func (row *Record) SetKind(value int) {
 row.Kind=value
 if row.Has==nil { row.Has=&RecordHas{} }
 row.Has.Kind=true
}
`

func borrowedAuthoredAccessorFixture(t *testing.T) (string, *Source, *Source, string) {
	t.Helper()
	root, owner, child, _ := borrowedAuxiliaryOwnerFixture(t)
	owner.GoBuild = &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GO111MODULE=on"}}
	child.GoBuild = owner.GoBuild.Clone()
	path := filepath.Join(root, "generated", "authored_accessor.go")
	require.NoError(t, os.WriteFile(path, []byte(borrowedAuthoredSetter), 0600))
	_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: owner, Destination: root})
	require.NoError(t, err)
	return root, owner, child, path
}

func TestBorrowedAuthoredSetterGeneratedGetter(t *testing.T) {
	root, owner, child, path := borrowedAuthoredAccessorFixture(t)
	before, err := os.ReadFile(path)
	require.NoError(t, err)
	_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
	require.NoError(t, err)
	entities, err := os.ReadFile(filepath.Join(root, "generated", "entities.go"))
	require.NoError(t, err)
	require.NotContains(t, string(entities), "func (entity *Record) SetKind(")
	require.Contains(t, string(entities), "func (entity *Record) GetKind() int")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	snapshot := borrowedGeneratedSnapshot(t, root)
	for _, source := range []*Source{owner, child} {
		_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
	}
	require.Equal(t, snapshot, borrowedGeneratedSnapshot(t, root), "repeat changed selected accessor artifacts")
	writeSourceFile(t, root, "generated/authored_accessor_runtime_test.go", `package generated
 import "testing"
 func TestSelectedAuthoredAccessor(t *testing.T){row:=&Record{};row.SetKind(17);if row.GetKind()!=17||row.Has==nil||!row.Has.Kind{t.Fatal("authored setter/generated getter")}}
 `)
	borrowedGeneratedModuleTest(t, root)
}

func TestBorrowedRejectsInvalidAuthoredAccessorOwnership(t *testing.T) {
	for _, kind := range []string{"wrong signature", "duplicate", "invalid body", "excluded"} {
		t.Run(kind, func(t *testing.T) {
			root, _, child, path := borrowedAuthoredAccessorFixture(t)
			source := borrowedAuthoredSetter
			switch kind {
			case "wrong signature":
				source = strings.Replace(source, "value int)", "value int, extra int)", 1)
			case "duplicate":
				require.NoError(t, os.WriteFile(filepath.Join(root, "generated", "duplicate_accessor.go"), []byte(source), 0600))
			case "invalid body":
				source = strings.Replace(source, "row.Kind=value", "row.Kind=missingSymbol", 1)
			case "excluded":
				source = "//go:build datly_excluded_accessor\n\n" + source
			}
			require.NoError(t, os.WriteFile(path, []byte(source), 0600))
			_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: child, Destination: root})
			require.Error(t, err, "invalid/excluded accessor supplied generated helper ownership")
		})
	}
}

func TestBorrowedEntirelyAuthoredReceiver(t *testing.T) {
	root, owner, child, _ := borrowedAuxiliaryOwnerFixture(t)
	owner.GoBuild = &gobuild.Context{Dir: root, Env: []string{"GOWORK=off", "GO111MODULE=on"}}
	child.GoBuild = owner.GoBuild.Clone()
	entities, err := os.ReadFile(filepath.Join(root, "generated", "entities.go"))
	require.NoError(t, err)
	file, err := parser.ParseFile(token.NewFileSet(), "entities.go", entities, 0)
	require.NoError(t, err)
	var source bytes.Buffer
	source.WriteString("package generated\n")
	count := 0
	for _, decl := range file.Decls {
		method, ok := decl.(*ast.FuncDecl)
		if !ok || method.Recv == nil || len(method.Recv.List) != 1 {
			continue
		}
		receiver, ok := method.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		name, ok := receiver.X.(*ast.Ident)
		if !ok || name.Name != "Record" {
			continue
		}
		var printed bytes.Buffer
		require.NoError(t, format.Node(&printed, token.NewFileSet(), method))
		// Distinct authored source retains the same public field API and body semantics.
		source.WriteString(strings.ReplaceAll(printed.String(), "entity", "row"))
		source.WriteString("\n")
		count++
	}
	require.Positive(t, count)
	path := filepath.Join(root, "generated", "authored_accessors.go")
	require.NoError(t, os.WriteFile(path, source.Bytes(), 0600))
	for _, input := range []*Source{owner, child} {
		_, err = (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: input, Destination: root})
		require.NoError(t, err)
	}
	selected, err := os.ReadFile(filepath.Join(root, "generated", "entities.go"))
	require.NoError(t, err)
	require.NotContains(t, string(selected), "func (entity *Record)", "fully authored receiver still expects generated methods")
	actual, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, source.Bytes(), actual)
	borrowedGeneratedModuleTest(t, root)
}

func TestBorrowedSelectedAccessorSealsAndStagedDrift(t *testing.T) {
	for _, kind := range []string{"authored", "generated"} {
		t.Run(kind, func(t *testing.T) {
			root, _, child, authored := borrowedAuthoredAccessorFixture(t)
			compiled, err := (&Compiler{}).Compile(t.Context(), child)
			require.NoError(t, err)
			var proof *gen.BorrowedRowAuthority
			for _, reference := range compiled.Views {
				if reference != nil && reference.Borrowed != nil {
					proof = reference.Borrowed
				}
			}
			require.NotNil(t, proof)
			found := false
			for _, seal := range proof.Files {
				if seal.Path == authored {
					found = true
				}
			}
			require.True(t, found, "selected authored accessor file is not sealed")
			target := filepath.Join(root, "generated")
			stage := target + ".staged"
			require.NoError(t, os.Rename(target, stage))
			defer func() { require.NoError(t, os.Rename(stage, target)) }()
			resolve := func(path string) string {
				if strings.HasPrefix(path, target+string(os.PathSeparator)) {
					return stage + strings.TrimPrefix(path, target)
				}
				return path
			}
			require.NoError(t, proof.ValidateFiles(resolve), "selected source must revalidate through existing staged path")
			changed := authored
			if kind == "generated" {
				changed = filepath.Join(target, "entities.go")
			}
			bytes, err := os.ReadFile(resolve(changed))
			require.NoError(t, err)
			if kind == "authored" {
				bytes = []byte(strings.Replace(string(bytes), "row.Kind=value", "row.Kind=value+1", 1))
			} else {
				bytes = []byte(strings.Replace(string(bytes), "return entity.Kind", "return entity.Kind + 1", 1))
			}
			require.NoError(t, os.WriteFile(resolve(changed), bytes, 0600))
			require.ErrorContains(t, proof.ValidateFiles(resolve), "content drift")
		})
	}
}
