package transcribe

import (
	"context"
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
	xshape "github.com/viant/x/shape"
)

func TestFactorySourceMembersNativeGeneration(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "shape.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT, EXCLUSION TEXT)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	generate := func() (*GeneratedPackage, error) {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		if err != nil {
			return nil, err
		}
		return (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	}
	_, err = generate()
	require.NoError(t, err)
	// Start with independent native authority, convert it to the authored initial
	// declaration and explicitly add five retained application members.
	input, err := os.ReadFile(filepath.Join(root, "archive/input.go"))
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Join(root, "archive/res.go"))
	require.NoError(t, err)
	a, err := parser.ParseFile(token.NewFileSet(), "input.go", input, 0)
	require.NoError(t, err)
	b, err := parser.ParseFile(token.NewFileSet(), "res.go", body, 0)
	require.NoError(t, err)
	var declarations []ast.Decl
	for _, declaration := range a.Decls {
		gen, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		if gen.Tok == token.IMPORT {
			declarations = append(declarations, gen)
			continue
		}
		for _, entry := range gen.Specs {
			typ, ok := entry.(*ast.TypeSpec)
			if !ok || typ.Name.Name != "ArchiveInput" {
				continue
			}
			structure := typ.Type.(*ast.StructType)
			var fields []*ast.Field
			for _, field := range structure.Fields.List {
				if field.Names[0].Name != "Has" {
					fields = append(fields, field)
				}
			}
			structure.Fields.List = fields
			declarations = append(declarations, gen)
		}
	}
	a.Decls = append(declarations, b.Decls...)
	combined, err := (xshape.SourceParser{}).FormatFile(a)
	require.NoError(t, err)
	combined, err = (xshape.SourceParser{}).AppendStructFields(combined, []byte("package archive\ntype ArchiveInput struct { Partial bool; origin bool }; type Res struct { records []*Res; InputHas *bool; projection *int }"))
	require.NoError(t, err)
	combined = append(combined, []byte("\nfunc (r *ArchiveInput) IsPartial() bool { return r.Partial }; func NewLocalInput() *ArchiveInput { return &ArchiveInput{Partial:true} }\n")...)
	require.NoError(t, os.Remove(filepath.Join(root, "archive/input.go")))
	require.NoError(t, os.Remove(filepath.Join(root, "archive/res.go")))
	require.NoError(t, os.Remove(filepath.Join(root, "archive/input_setters.go")))
	writeSourceHandlerFile(t, root, "archive/route_input.go", string(combined))
	source.Text = strings.Replace(source.Text, "dest(a,'res.go')", "dest(a,'route_input.go')", 1)
	source.Text += `
#setting($_ = $input_dest('route_input.go'))
#setting($_ = $source_input('ArchiveInput'))
#setting($_ = $source_member('ArchiveInput','Partial'))
#setting($_ = $source_member('ArchiveInput','origin'))
#setting($_ = $source_member('Res','records'))
#setting($_ = $source_member('Res','InputHas'))
#setting($_ = $source_member('Res','projection'))
`
	// Independently generated field mismatch fails initial admission atomically.
	invalid := strings.Replace(string(combined), "InpData", "UnknownInput", 1)
	writeSourceHandlerFile(t, root, "archive/route_input.go", invalid)
	before := sourceHandlerSnapshot(t, root)
	_, err = generate()
	require.Error(t, err)
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
	writeSourceHandlerFile(t, root, "archive/route_input.go", string(combined))
	var first map[string]string
	for round := 0; round < 2; round++ {
		result, err := generate()
		require.NoError(t, err)
		require.Empty(t, result.Result.Plan.RootSource)
		require.Empty(t, result.Result.Plan.Connector)
		require.Nil(t, result.Result.Plan.MutationHandler)
		current := sourceHandlerSnapshot(t, root)
		if round == 0 {
			first = current
		} else {
			require.Equal(t, first, current)
		}
		content := current[filepath.Join(root, "archive/route_input.go")]
		require.Contains(t, content, "func NewLocalInput()")
		require.Contains(t, content, "func (r *ArchiveInput) IsPartial()")
		fields, err := (xshape.SourceParser{}).Parse([]byte(content))
		require.NoError(t, err)
		var inputNames []string
		for _, field := range fields.Fields {
			if field.Owner == "ArchiveInput" {
				inputNames = append(inputNames, field.Names...)
			}
		}
		require.Equal(t, []string{"InpData", "Partial", "origin"}, inputNames)
		require.NoFileExists(t, filepath.Join(root, "archive/input_setters.go"))
	}
	// Same-owner generated edits do not become retention authority.
	path := filepath.Join(root, "archive/route_input.go")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data, err = (xshape.SourceParser{}).AppendStructFields(data, []byte("package archive\ntype Res struct { Unselected string }"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0600))
	_, err = generate()
	require.NoError(t, err)
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.NotContains(t, string(data), "Unselected")
	// Fresh physical schema and SQL projection evolve generated authority while
	// all five original Go source members survive without edits.
	_, err = db.Exec("ALTER TABLE CI_AUDIENCE ADD COLUMN REVISION INTEGER")
	require.NoError(t, err)
	source.Text = strings.Replace(source.Text, "a.TARGET AS Target,", "a.TARGET AS Target,a.REVISION AS Revision,", 1)
	_, err = generate()
	require.NoError(t, err)
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(data), "Revision")
	require.Contains(t, string(data), "projection")
	before = sourceHandlerSnapshot(t, root)
	source.Text = strings.Replace(source.Text, "'Res','projection'", "'Res','missing'", 1)
	_, err = generate()
	require.ErrorContains(t, err, "stale")
	require.Equal(t, before, sourceHandlerSnapshot(t, root))

}
