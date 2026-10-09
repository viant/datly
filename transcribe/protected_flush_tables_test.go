package transcribe

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestProtectedFlushTablesPackageOverlayAuthority(t *testing.T) {
	base := &spec.Settings{ProtectedFlushTables: []string{"package_records"}}
	authored := &spec.Settings{ProtectedFlushTables: []string{"records", "records/attributes"}}
	loaded := (&settingsLoader{base: base, authored: authored}).Load()
	require.Equal(t, authored.ProtectedFlushTables, loaded.ProtectedFlushTables)
	loaded.ProtectedFlushTables[0] = "changed"
	require.Equal(t, "records", authored.ProtectedFlushTables[0])
	require.Equal(t, "package_records", base.ProtectedFlushTables[0])

	compiled, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/records", Name: "Records",
		PackageComponent: &spec.Component{Settings: base},
		Text:             "#setting($_ = $route('/records','POST'))\n#setting($_ = $protected_flush_tables('records'))\nSELECT id FROM records",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"records"}, compiled.Component.Settings.ProtectedFlushTables)
	require.Equal(t, "package_records", base.ProtectedFlushTables[0])
}

func TestProtectedFlushTablesDQLRemovalRevokesPackageGrant(t *testing.T) {
	base := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Records"}, Name: "Records",
		Settings: &spec.Settings{ProtectedFlushTables: []string{"records"}, CaseFormat: "lc"},
		Routes:   []*spec.Route{{Method: "POST", Path: "/records"}},
	}
	for _, text := range []string{
		"#setting($_ = $route('/records','POST'))\nSELECT id FROM records",
		"#setting($_ = $route('/records','POST'))\n#setting($_ = $case_format('lc'))\nSELECT id FROM records",
	} {
		compiled, err := NewCompiler().Compile(context.Background(), &Source{Scope: base.Key.Scope, Name: base.Name, PackageComponent: base, Text: text})
		require.NoError(t, err)
		require.Nil(t, compiled.Component.Settings.ProtectedFlushTables)
		require.Equal(t, "lc", compiled.Component.Settings.CaseFormat)
		require.Equal(t, []string{"records"}, base.Settings.ProtectedFlushTables)
	}
	packageOnly, err := NewCompiler().Compile(context.Background(), &Source{Scope: base.Key.Scope, Name: base.Name, PackageComponent: base})
	require.NoError(t, err)
	require.Equal(t, []string{"records"}, packageOnly.Component.Settings.ProtectedFlushTables)
}

func TestProtectedFlushTablesNativeFactoryGenerationReload(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	source.Text = strings.Replace(source.Text, "#setting($_ = $route('/archive','POST'))", "#setting($_ = $route('/archive','POST'))\n#setting($_ = $protected_flush_tables('records','records/attributes'))", 1)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
	require.NoError(t, err)
	require.Nil(t, compiled.Component.RootView)
	require.Equal(t, []string{"records", "records/attributes"}, compiled.Component.Settings.ProtectedFlushTables)
	generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	require.NoError(t, err)
	require.Equal(t, []string{"records", "records/attributes"}, generated.Result.Plan.Settings.ProtectedFlushTables)
	runtimeTest := `package archive
import (
 "reflect"
 "testing"
 "github.com/viant/datly/bootstrap"
 dtag "github.com/viant/datly/tag"
)
func TestGeneratedProtectedFlushMetadata(t *testing.T) {
 wantTables:=[]string{"records","records/attributes"}
 field,ok:=reflect.TypeFor[ArchiveComponent]().FieldByName("Contract");if !ok{t.Fatal("missing factory contract holder")}
 metadata,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatalf("generated component tag: %v",err)}
 component,err:=(&bootstrap.RouteSource{PackagePath:"github.com/viant/datly/handlerfixture/archive",Tag:metadata}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());if err!=nil{t.Fatal(err)}
 if component.Settings==nil||component.Settings.IsZero()||!reflect.DeepEqual(component.Settings.ProtectedFlushTables,wantTables){t.Fatalf("lost native factory table authority: %+v",component.Settings)}
 if err=component.Settings.ValidateProtectedFlushTables();err!=nil{t.Fatal(err)}
 clone:=component.Clone();component.Settings.ProtectedFlushTables[0]="changed"
 if len(wantTables)>0{if clone.Settings.ProtectedFlushTables[0]!="records"{t.Fatal("native clone aliases table authority")}}
}
`
	// Exercise both generation with grants and actual regeneration after removal.
	for round := 0; round < 2; round++ {
		if round == 1 {
			source.Text = strings.Replace(source.Text, "#setting($_ = $protected_flush_tables('records','records/attributes'))\n", "", 1)
			compiled, err = (&Discovery{BaseDir: root, GoBuild: source.GoBuild}).CompileSource(context.Background(), source)
			require.NoError(t, err)
			require.Nil(t, compiled.Component.Settings.ProtectedFlushTables)
			generated, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
			require.NoError(t, err)
			require.Nil(t, generated.Result.Plan.Settings.ProtectedFlushTables)
			runtimeTest = strings.Replace(runtimeTest, `wantTables:=[]string{"records","records/attributes"}`, `var wantTables []string`, 1)
			runtimeTest = strings.Replace(runtimeTest, `component.Settings.ProtectedFlushTables[0]="changed"`, `if len(wantTables)>0{component.Settings.ProtectedFlushTables[0]="changed"}`, 1)
		}
		writeSourceHandlerFile(t, root, "archive/protected_metadata_test.go", runtimeTest)
		cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-count=1", "-timeout=2m", "-run", "^TestGeneratedProtectedFlushMetadata$", "./archive")
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
}
