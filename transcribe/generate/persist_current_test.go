package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	xshape "github.com/viant/x/shape"
)

func TestCurrentContractRegeneration(t *testing.T) {
	type usecase struct {
		name     string
		input    []Field
		expected []string
	}
	cases := []usecase{
		{"field add", []Field{{Name: "Id", Type: "int"}, {Name: "Name", Type: "string"}}, []string{"Id:int", "Name:string"}},
		{"field remove", []Field{{Name: "Id", Type: "int"}}, []string{"Id:int"}},
		{"nullable cast", []Field{{Name: "Id", Type: "*int", ExplicitType: true}}, []string{"Id:*int"}},
		{"tag change", []Field{{Name: "Id", Type: "int", Tag: `sqlx:"id,primaryKey" validate:"required"`}}, []string{"Id:int"}},
		{"relation cardinality", []Field{{Name: "Child", Type: "*Child", Tag: `on:"Id:ParentId"`, RelationHolder: true}}, []string{"Child:*Child"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", RouterDest: "router.go", ViewDest: "views.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Views: []ViewPlan{
				{Name: "Row", Type: "Row", Destination: "views.go", Ownership: ViewGenerated, Fields: []Field{{Name: "Id", Type: "int"}, {Name: "Name", Type: "string"}, {Name: "Child", Type: "[]*Child", RelationHolder: true}}, SetMarkerFields: []string{"Id", "Name"}},
				{Name: "Child", Type: "Child", Destination: "views.go", Ownership: ViewGenerated, Fields: []Field{{Name: "ParentId", Type: "int"}}},
			}}
			_, err := EmitScaffold(dir, plan)
			require.NoError(t, err)
			path := filepath.Join(dir, "views.go")
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			// Edits inside generated files have no persistent ownership protection.
			require.NoError(t, os.WriteFile(path, append(data, []byte("\nfunc(Row) DirectEdit() {}\n")...), 0644))
			hook := "package records\nfunc(Row) ApplicationHook() {}\n"
			require.NoError(t, os.WriteFile(filepath.Join(dir, "hooks.go"), []byte(hook), 0644))
			plan.Views[0].Fields = tc.input
			plan.Views[0].SetMarkerFields = []string{"Id"}
			_, err = EmitScaffold(dir, plan)
			require.NoError(t, err)
			parsed, err := (xshape.SourceParser{}).ParseFile(path)
			require.NoError(t, err)
			var got []string
			for _, f := range parsed.Fields {
				if f.Owner == "Row" {
					got = append(got, strings.Join(f.Names, ",")+":"+f.TypeExpr)
				}
			}
			require.Equal(t, tc.expected, got)
			data, err = os.ReadFile(path)
			require.NoError(t, err)
			require.NotContains(t, string(data), "DirectEdit")
			for _, f := range tc.input {
				if f.Tag != "" {
					require.Contains(t, string(data), f.Tag)
				}
			}
			data, err = os.ReadFile(filepath.Join(dir, "hooks.go"))
			require.NoError(t, err)
			require.Equal(t, hook, string(data))
			before, err := readScaffoldSnapshot(dir)
			require.NoError(t, err)
			_, err = EmitScaffold(dir, plan)
			require.NoError(t, err)
			after, err := readScaffoldSnapshot(dir)
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, err = os.Stat(filepath.Join(dir, legacyManifestName))
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestCurrentArtifactsAndResourcesAreRetired(t *testing.T) {
	dir := t.TempDir()
	first := &Plan{Package: "example.com/records", ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Resources: &ResourcePlan{
		Namespace: readableResourceNamespace("example.com/records", "Records"), Destination: "resources.go", Files: []EmittedFile{{Path: "sql/old.sql", Content: "SELECT 1"}, {Path: "sql/shared.sql", Content: "SELECT 2"}},
	}}
	_, err := EmitScaffold(dir, first)
	require.NoError(t, err)
	// The other namespace explicitly shares one asset and owns another.
	other := `package records
import "embed"
const OtherDatlyResourceNamespace="other"
//go:embed sql/shared.sql sql/other.sql
var OtherDatlyResources embed.FS
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "other.go"), []byte(other), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sql/other.sql"), []byte("SELECT 3"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "obsolete.go"), []byte(generatedHeader("Records")+"package records\nconst Obsolete=1\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "foreign.go"), []byte(generatedHeader("Other")+"package records\nconst Foreign=1\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sql/old.sql"), []byte("SELECT 42 -- edited"), 0644))
	first.Resources.Files = []EmittedFile{{Path: "sql/new.sql", Content: "SELECT 4"}}
	_, err = EmitScaffold(dir, first)
	require.NoError(t, err)
	for _, name := range []string{"sql/old.sql", "obsolete.go"} {
		_, err = os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(err), name)
	}
	for _, name := range []string{"sql/shared.sql", "sql/other.sql", "foreign.go", "other.go"} {
		_, err = os.Stat(filepath.Join(dir, name))
		require.NoError(t, err, name)
	}
	first.Resources = nil
	_, err = EmitScaffold(dir, first)
	require.NoError(t, err)
	for _, name := range []string{"resources.go", "sql/new.sql"} {
		_, err = os.Stat(filepath.Join(dir, name))
		require.True(t, os.IsNotExist(err), name)
	}
}

func TestLinkedContractWithDefaultFilenameIsPreserved(t *testing.T) {
	dir := t.TempDir()
	authored := "package records\ntype Input struct {Application string}\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "input.go"), []byte(authored), 0644))
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: ContractPlan{Type: "Input", Destination: "input.go", Ownership: ContractLinked}, Output: generatedContract("Output", "output.go")}
	_, err := EmitScaffold(dir, plan)
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(dir, "input.go"))
	require.NoError(t, err)
	require.Equal(t, authored, string(data))
}

func TestLegacyGeneratedSourceUpgrade(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{Package: "example.com/records", ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go"), Resources: &ResourcePlan{Namespace: "records", Destination: "resources.go", Files: []EmittedFile{{Path: "sql/query.sql", Content: "SELECT 1"}}}}
	_, err := EmitScaffold(dir, plan)
	require.NoError(t, err)
	// Model older outputs without standard generated headers. The ordinary
	// scaffold comments and current AST provide a one-time migration path.
	for _, name := range []string{"router.go", "input.go", "output.go", "resources.go"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, []byte(strings.TrimPrefix(string(data), generatedHeader("Records"))), 0644))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, legacyManifestName), []byte("{malformed"), 0644))
	_, err = EmitScaffold(dir, plan)
	require.NoError(t, err)
	for _, name := range []string{"router.go", "input.go", "output.go", "resources.go"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		require.Equal(t, "Records", generatedOwner(data))
	}
	_, err = os.Stat(filepath.Join(dir, legacyManifestName))
	require.True(t, os.IsNotExist(err))
}

func TestMalformedGeneratedShapeIsReplaceable(t *testing.T) {
	dir := t.TempDir()
	plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
	_, err := EmitScaffold(dir, plan)
	require.NoError(t, err)
	path := filepath.Join(dir, "input.go")
	require.NoError(t, os.WriteFile(path, []byte(generatedHeader("Records")+"package records\ntype Input struct {not valid"), 0644))
	_, err = EmitScaffold(dir, plan)
	require.NoError(t, err)
	_, err = (xshape.SourceParser{}).ParseFile(path)
	require.NoError(t, err)
}

func TestLegacyMethodOnlyArtifactUpgrade(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		upgrade      bool
	}{
		{"setter", `func(input *Input)SetId(value int){input.Id=value}`, true},
		{"authored hook", `func(input *Input)Init()error{return nil}`, false},
		{"foreign receiver", `func(input *Other)SetId(value int){}`, false},
		{"extra authored declaration", `func(input *Input)SetId(value int){input.Id=value};const ApplicationOwned=true`, false},
		{"application init", `func(input *Input)SetId(value int){input.Id=value};func init(){}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			plan := &Plan{ComponentName: "Records", RouterDest: "router.go", Input: generatedContract("Input", "input.go", Field{Name: "Id", Type: "int", Tag: `parameter:"id,kind=query,in=id"`}), Output: generatedContract("Output", "output.go")}
			_, err := EmitScaffold(dir, plan)
			require.NoError(t, err)
			// The old generated input/holder comments remain, but support files have no
			// generated-file header. Native source identity must recognize their methods.
			path := filepath.Join(dir, "input_setters.go")
			old := "package records\n" + tc.source + "\n"
			require.NoError(t, os.WriteFile(path, []byte(old), 0644))
			_, err = EmitScaffold(dir, plan)
			if tc.upgrade {
				require.NoError(t, err)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, "Records", generatedOwner(data))
			} else {
				require.ErrorContains(t, err, "unowned package file")
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, old, string(data))
			}
		})
	}
}
