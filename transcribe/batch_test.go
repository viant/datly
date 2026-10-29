package transcribe

import (
	"fmt"
	"go/ast"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func batchFixture(t testing.TB, root string) {
	t.Helper()
	// Keep the benchmark independent of database discovery and live services.
	data := "module example.com/batch\n\ngo 1.25\n"
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte(data), 0600))
	for _, dir := range []string{"source", "model", "ignored"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "model/claims.go"), []byte("package model\ntype Claims struct { Subject string }"), 0600))
	for _, name := range []string{"a", "b"} {
		text := fmt.Sprintf(`#package('example.com/batch/api/%s')
#import('model','example.com/batch/model')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/%s','GET'))
#define($_ = $Claims<*model.Claims>(param/Claims).Optional())
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row'), CAST(r.ID AS int)
FROM (SELECT 1 AS ID) r`, name, name)
		require.NoError(t, os.WriteFile(filepath.Join(root, "source", name+".dql"), []byte(text), 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "ignored/broken.dql"), []byte("#setting($_ = $route('/broken','GET'))\nSELECT * FROM (SELECT"), 0600))
}

func batchOutputs(t testing.TB, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	require.NoError(t, filepath.WalkDir(filepath.Join(root, "api"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[relative] = strings.ReplaceAll(string(data), root, "<ROOT>")
		return nil
	}))
	return result
}

func TestTranscribeBatchEquivalence(t *testing.T) {
	for _, selection := range []string{"files", "directory", "packages"} {
		t.Run(selection, func(t *testing.T) {
			root, single := t.TempDir(), t.TempDir()
			for _, dir := range []string{root, single} {
				batchFixture(t, dir)
				(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, dir)
			}
			for _, name := range []string{"a", "b"} {
				d := &Discovery{BaseDir: single, Include: []string{"example.com/batch/source"}, SourceFile: name + ".dql"}
				project, err := d.Compile(t.Context())
				require.NoError(t, err)
				_, err = (Generator{Operation: "get"}).Generate(t.Context(), GenerationRequest{Compiled: project.Components[0], Destination: single})
				require.NoError(t, err)
			}
			includes := []string{"source/a.dql", "source/b.dql"}
			if selection == "directory" {
				includes = []string{"source"}
			}
			if selection == "packages" {
				includes = []string{"example.com/batch/source/..."}
			}
			result, err := (&Discovery{BaseDir: root, Include: includes}).TranscribeBatch(t.Context(), BatchRequest{Generator: Generator{Operation: "get"}, Destination: root})
			require.NoError(t, err)
			require.Len(t, result.Packages, 2)
			require.Equal(t, 1, result.DependencyParses)
			require.GreaterOrEqual(t, result.DependencyReuses, 1)
			require.Equal(t, batchOutputs(t, single), batchOutputs(t, root))
		})
	}
}

func TestTranscribeBatchSelectionAndErrors(t *testing.T) {
	for _, test := range []struct {
		name             string
		include, exclude []string
		ignore           string
		want             int
		errorText        string
	}{
		{name: "exclude", include: []string{"example.com/batch/..."}, exclude: []string{"example.com/batch/ignored/..."}, want: 2},
		{name: "goignore", include: []string{"."}, ignore: "ignored\n", want: 2},
		{name: "ordered dedup", include: []string{"source/b.dql", "source/a.dql", "source/b.dql"}, want: 2},
		{name: "partial error", include: []string{"source/a.dql", "ignored/broken.dql"}, want: 1, errorText: "broken.dql"},
		{name: "empty", include: []string{"model"}, errorText: "no component sources"},
		{name: "missing", include: []string{"source/missing.dql"}, errorText: "missing.dql"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			batchFixture(t, root)
			(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, root)
			if test.ignore != "" {
				require.NoError(t, os.WriteFile(filepath.Join(root, ".goignore"), []byte(test.ignore), 0600))
			}
			result, err := (&Discovery{BaseDir: root, Include: test.include, Exclude: test.exclude}).TranscribeBatch(t.Context(), BatchRequest{Generator: Generator{Operation: "get"}, Destination: root})
			if test.errorText != "" {
				require.ErrorContains(t, err, test.errorText)
			} else {
				require.NoError(t, err)
			}
			if result != nil {
				require.Len(t, result.Packages, test.want)
			} else {
				require.Zero(t, test.want)
			}
			if test.name == "ordered dedup" {
				require.Equal(t, "b.dql", filepath.Base(result.Sources[0]))
			}
		})
	}
}

func TestTranscribeBatchGeneratedDependency(t *testing.T) {
	root := t.TempDir()
	batchFixture(t, root)
	(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, root)
	path := filepath.Join(root, "source/b.dql")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	text := strings.ReplaceAll(string(data), "#import('model','example.com/batch/model')", "#import('model','example.com/batch/api/a')")
	text = strings.ReplaceAll(text, "*model.Claims", "*model.Row")
	require.NoError(t, os.WriteFile(path, []byte(text), 0600))
	result, err := (&Discovery{BaseDir: root, Include: []string{"source/a.dql", "source/b.dql"}}).TranscribeBatch(t.Context(), BatchRequest{Generator: Generator{Operation: "get"}, Destination: root})
	require.NoError(t, err)
	require.Len(t, result.Packages, 2)
	generated, err := os.ReadFile(filepath.Join(root, "api/b/input.go"))
	require.NoError(t, err)
	require.Contains(t, string(generated), "*model.Row")
	command := testharness.SourceGoCommand(t, root, "test", "./api/...")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestTranscribeBatchWriterReaderDependencies(t *testing.T) {
	for _, operation := range []string{"patch", "post", "put"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			batchFixture(t, root)
			(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, root)
			db := testharness.NewSQLiteHarness(t)
			require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE RECORDS(ID INTEGER PRIMARY KEY, NAME TEXT)"))
			owner := fmt.Sprintf(`#package('example.com/batch/api/a')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/a','%s'))
SELECT r.*, type(r,'Row') FROM RECORDS r`, strings.ToUpper(operation))
			require.NoError(t, os.WriteFile(filepath.Join(root, "source/a.dql"), []byte(owner), 0600))
			data, err := os.ReadFile(filepath.Join(root, "source/b.dql"))
			require.NoError(t, err)
			consumer := strings.ReplaceAll(string(data), "#import('model','example.com/batch/model')", "#import('model','example.com/batch/api/a')")
			consumer = strings.ReplaceAll(consumer, "*model.Claims", "*model.Row")
			require.NoError(t, os.WriteFile(filepath.Join(root, "source/b.dql"), []byte(consumer), 0600))
			result, err := (&Discovery{BaseDir: root, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}).TranscribeBatch(t.Context(), BatchRequest{Generator: Generator{Operation: "get"}, Destination: root, Rules: []BatchRule{{Source: "source/a.dql", Operation: operation}, {Source: "source/b.dql"}}})
			require.NoError(t, err)
			require.Len(t, result.Packages, 2)
			output, err := testharness.SourceGoCommand(t, root, "test", "./api/...").CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}

func TestTranscribeBatchRefreshAfterGeneration(t *testing.T) {
	root := t.TempDir()
	batchFixture(t, root)
	(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, root)
	d := &Discovery{BaseDir: root, Include: []string{"source/a.dql"}}
	request := BatchRequest{Generator: Generator{Operation: "get"}, Destination: root}
	_, err := d.TranscribeBatch(t.Context(), request)
	require.NoError(t, err)
	consumerData, err := os.ReadFile(filepath.Join(root, "source/b.dql"))
	require.NoError(t, err)
	consumer := strings.ReplaceAll(string(consumerData), "#import('model','example.com/batch/model')", "#import('model','example.com/batch/api/a')")
	consumer = strings.ReplaceAll(consumer, "*model.Claims", "*model.Input")
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/b.dql"), []byte(consumer), 0600))
	before := strings.ReplaceAll(consumer, "api/b", "api/c")
	before = strings.ReplaceAll(before, "'/b'", "'/c'")
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/c.dql"), []byte(before), 0600))
	owner, err := os.ReadFile(filepath.Join(root, "source/a.dql"))
	require.NoError(t, err)
	ownerText := strings.ReplaceAll(string(owner), "#define($_ = $Data", "#define($_ = $Extra<string>(query/extra).Optional())\n#define($_ = $Data")
	require.NoError(t, os.WriteFile(filepath.Join(root, "source/a.dql"), []byte(ownerText), 0600))
	d.Include = []string{"source/c.dql", "source/a.dql", "source/b.dql"}
	result, err := d.TranscribeBatch(t.Context(), request)
	require.NoError(t, err)
	old, _, err := result.Packages[0].Types.Resolve(typecatalog.PackageAuthority, "example.com/batch/api/a.Input")
	require.NoError(t, err)
	require.NotNil(t, old)
	hasExtra := func(typ *ast.TypeSpec) bool {
		for _, field := range typ.Type.(*ast.StructType).Fields.List {
			for _, name := range field.Names {
				if name.Name == "Extra" {
					return true
				}
			}
		}
		return false
	}
	found := hasExtra(old.SynteticType.TypeSpec)
	require.False(t, found)
	fresh, _, err := result.Packages[2].Types.Resolve(typecatalog.PackageAuthority, "example.com/batch/api/a.Input")
	require.NoError(t, err)
	require.NotNil(t, fresh)
	found = hasExtra(fresh.SynteticType.TypeSpec)
	require.True(t, found, "later consumer must see the newly generated input field")
	require.Greater(t, result.DependencyReuses, 0)
}

// This causal sequence checks that regenerating one owner refreshes its alias
// without making the other owner's changed database schema visible prematurely.
func TestTranscribeBatchRefreshesRowOwnersThroughAliases(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/batch"}).Write(t, root)
	write := func(name, source string) {
		t.Helper()
		target := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(target), 0755))
		require.NoError(t, os.WriteFile(target, []byte(source), 0600))
	}
	db := testharness.NewSQLiteHarness(t)
	type ownerFixture struct{ name, table, row, alias, column, field string }
	owners := []ownerFixture{
		{"books", "BOOKS", "BookView", "Book", "EDITION TEXT", "Edition"},
		{"shelves", "SHELVES", "ShelfView", "Shelf", "CAPACITY INTEGER", "Capacity"},
	}
	var seedRules []BatchRule
	for _, owner := range owners {
		require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE "+owner.table+"(ID INTEGER PRIMARY KEY)"))
		source := fmt.Sprintf(`#package('example.com/batch/api/%s')
#setting($_ = $input_type('%sInput'))
#setting($_ = $output_type('%sOutput'))
#setting($_ = $route('/%s','GET'))
#define($_ = $Data<[]*%s>(output/view))
SELECT r.*, type(r,'%s') FROM %s r`, owner.name, owner.alias, owner.alias, owner.name, owner.row, owner.row, owner.table)
		file := "source/" + owner.name + ".dql"
		write(file, source)
		seedRules = append(seedRules, BatchRule{Source: file})
	}
	discovery := &Discovery{BaseDir: root, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	request := BatchRequest{Generator: Generator{Operation: "get"}, Destination: root, Rules: seedRules}
	_, err := discovery.TranscribeBatch(t.Context(), request)
	require.NoError(t, err)
	aliases := map[string]string{}
	for _, owner := range owners {
		name := "model/" + owner.name + ".go"
		aliases[name] = fmt.Sprintf("package model\nimport %s \"example.com/batch/api/%s\"\ntype %s %s.%s\n", owner.name, owner.name, owner.alias, owner.name, owner.row)
		write(name, aliases[name])
		require.NoError(t, db.ExecStatements(t.Context(), "ALTER TABLE "+owner.table+" ADD COLUMN "+owner.column))
	}
	for _, name := range []string{"warm", "middle", "final"} {
		write("source/"+name+".dql", fmt.Sprintf(`#package('example.com/batch/api/%s')
#import('model','example.com/batch/model')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/%s','GET'))
#define($_ = $Book<*model.Book>(param/Book).Optional())
#define($_ = $Shelf<*model.Shelf>(param/Shelf).Optional())
#define($_ = $Data<[]*Row>(output/view))
SELECT r.*, type(r,'Row'), CAST(r.ID AS int) FROM (SELECT 1 AS ID) r`, name, name))
	}
	request.Rules = []BatchRule{{Source: "source/warm.dql"}, seedRules[0], {Source: "source/middle.dql"}, seedRules[1], {Source: "source/final.dql"}}
	result, err := discovery.TranscribeBatch(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Packages, 5)
	type useCase struct {
		description string
		index       int
		fields      []bool
	}
	for _, test := range []useCase{
		{"warm consumer sees both old owners", 0, []bool{false, false}},
		{"middle consumer sees only first regenerated owner", 2, []bool{true, false}},
		{"final consumer sees both regenerated owners", 4, []bool{true, true}},
	} {
		t.Run(test.description, func(t *testing.T) {
			catalog := result.Packages[test.index].Types
			for i, owner := range owners {
				key := "example.com/batch/api/" + owner.name + "."
				alias, found, err := catalog.Resolve(typecatalog.PackageAuthority, "example.com/batch/model."+owner.alias)
				require.NoError(t, err)
				require.True(t, found)
				require.NotNil(t, alias.SynteticType)
				target, ok := alias.SynteticType.TypeSpec.Type.(*ast.SelectorExpr)
				require.True(t, ok, "authored named type must still refer to its generated row")
				require.Equal(t, owner.row, target.Sel.Name)
				qualifier, ok := target.X.(*ast.Ident)
				require.True(t, ok)
				require.Equal(t, owner.name, qualifier.Name)
				row, found, err := catalog.Resolve(typecatalog.PackageAuthority, key+target.Sel.Name)
				require.NoError(t, err)
				require.True(t, found)
				require.NotNil(t, row.SynteticType)
				shape, ok := row.SynteticType.TypeSpec.Type.(*ast.StructType)
				require.True(t, ok)
				hasField := false
				for _, field := range shape.Fields.List {
					for _, name := range field.Names {
						if name.Name == owner.field {
							hasField = true
						}
					}
				}
				require.Equal(t, test.fields[i], hasField, "%s.%s", owner.row, owner.field)
			}
		})
	}
	require.Greater(t, result.DependencyReuses, 0, "the test must exercise a populated batch cache")
	for name, source := range aliases {
		actual, err := os.ReadFile(filepath.Join(root, name))
		require.NoError(t, err)
		require.Equal(t, source, string(actual), "authored aliases must survive regeneration")
	}
	input, err := os.ReadFile(filepath.Join(root, "api/final/input.go"))
	require.NoError(t, err)
	require.Contains(t, string(input), "*model.Book")
	require.Contains(t, string(input), "*model.Shelf")
	write("api/final/alias_fields_test.go", `package final
import (
 "example.com/batch/model"
)
var _ = (&model.Book{}).Edition
var _ = (&model.Shelf{}).Capacity
`)
	output, err := testharness.SourceGoCommand(t, root, "test", "./api/...").CombinedOutput()
	require.NoError(t, err, string(output))
}

func TestTranscribeBatchPreflight(t *testing.T) {
	for _, test := range []struct {
		name                  string
		rules                 []BatchRule
		sourceName, errorText string
	}{
		{"empty source", []BatchRule{{Source: ""}}, "", "empty batch source"},
		{"invalid operation", []BatchRule{{Source: "source/a.dql", Operation: "delete"}}, "", "invalid batch operation"},
		{"conflicting duplicate", []BatchRule{{Source: "source/a.dql", Operation: "get"}, {Source: "source/a.dql", Operation: "patch"}}, "", "conflicting operations"},
		{"ambiguous name", []BatchRule{{Source: "source"}}, "Override", "source-name requires exactly one"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			batchFixture(t, root)
			_, err := (&Discovery{BaseDir: root, SourceName: test.sourceName}).TranscribeBatch(t.Context(), BatchRequest{Generator: Generator{Operation: "get"}, Destination: root, Rules: test.rules})
			require.ErrorContains(t, err, test.errorText)
			_, err = os.Stat(filepath.Join(root, "api"))
			require.True(t, os.IsNotExist(err))
		})
	}
}
