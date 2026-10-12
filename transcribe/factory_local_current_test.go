package transcribe

import (
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

const factoryLocalRecord = "package archive\ntype Record struct { Id int `sqlx:\"ID\"`; Value string `sqlx:\"VALUE\"`; Has *RecordHas `setMarker:\"true\" sqlx:\"-\" json:\"-\"` }\ntype RecordHas struct { Id, Value bool }\n"

func localCurrentBodyFixture(t *testing.T, expression, options string) (string, *Source, *sql.DB) {
	t.Helper()
	root, source := factoryInputShapeFixture(t)
	writeSourceHandlerFile(t, root, "archive/record.go", factoryLocalRecord)
	source.Text = strings.Replace(source.Text, "#define($_ = $Status", fmt.Sprintf("#define($_ = $Current<%s>(view/Current).Cardinality('Many')%s /* SELECT ID,VALUE FROM RECORDS */)\n#define($_ = $Status", expression, options), 1)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT); CREATE TABLE RECORDS(ID INTEGER NOT NULL,VALUE TEXT NOT NULL); CREATE TABLE CANDIDATES(ID INTEGER NOT NULL)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	return root, source, db
}

func TestFactoryBodyAuthoredLocalCurrentNativeGeneration(t *testing.T) {
	for _, spelling := range []string{"Record", "archive.Record", handlerFixtureModule + "/archive.Record"} {
		t.Run(spelling, func(t *testing.T) {
			root, source, db := localCurrentBodyFixture(t, "[]*"+spelling, "")
			for _, pkg := range []string{"first", "second"} {
				writeSourceHandlerFile(t, root, pkg+"/record.go", "package "+pkg+"\nimport \"reflect\"\ntype Record struct { Foreign string }\nvar _anchorRecord = reflect.TypeFor[Record]()\n")
				source.Text = strings.Replace(source.Text, "#setting", fmt.Sprintf("#import(%q,%q)\n#setting", pkg, handlerFixtureModule+"/"+pkg), 1)
			}
			// Both generated independent reads and explicitly typed shared body
			// reads must keep fresh SQL authority on the first and later builds.
			source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $Candidates<?>(view/Candidates).Cardinality('Many').TypeName('Candidate') /* SELECT * FROM CANDIDATES */)
#define($_ = $BodyCurrent<[]*archive.Res>(view/BodyCurrent).Cardinality('Many').TypeName('archive.Res').Dest('res.go') /* SELECT TARGET,EXCLUSION FROM CI_AUDIENCE */)
#define($_ = $Caller<[]*archive.Record>(param/Caller).Optional())
#define($_ = $Status`, 1)
			source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION) FROM CI_AUDIENCE a`, 1)
			before := sourceHandlerSnapshot(t, root)
			var stable map[string]string
			for round := 0; round < 3; round++ {
				if round == 2 {
					_, err := db.Exec("ALTER TABLE CI_AUDIENCE ADD COLUMN EXTRA TEXT; ALTER TABLE CANDIDATES ADD COLUMN NOTE TEXT")
					require.NoError(t, err)
				}
				compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
				require.NoError(t, err)
				require.Nil(t, compiled.Component.RootView)
				require.NotNil(t, compiled.ExternalHandler.InputShape)
				request := GenerationRequest{Compiled: compiled, Destination: root}
				if round == 1 {
					request = GenerationRequest{Source: compiled.Source, Destination: root}
				}
				generated, err := (Generator{Operation: "post"}).Generate(t.Context(), request)
				require.NoError(t, err)
				linked, candidates := 0, 0
				for _, view := range generated.Result.Plan.Views {
					if view.DescriptorKey == handlerFixtureModule+"/archive.Record" {
						require.Equal(t, gen.ViewLinked, view.Ownership)
						require.Empty(t, view.Destination)
						linked++
					}
					if view.Type == "Candidate" {
						candidates++
						_, hasNote := view.Field("Note")
						require.Equal(t, round == 2, hasNote, "generated independent Current follows its own schema")
					}
				}
				require.Equal(t, 1, linked)
				require.Equal(t, 1, candidates)
				snapshot := sourceHandlerSnapshot(t, root)
				for _, file := range []string{"archive/record.go", "archive/business.go"} {
					require.Equal(t, before[filepath.Join(root, file)], snapshot[filepath.Join(root, file)])
				}
				if round == 0 {
					stable = snapshot
				} else if round == 1 {
					require.Equal(t, stable, snapshot)
				} else {
					require.Contains(t, snapshot[filepath.Join(root, "archive/res.go")], "Extra")
					require.NotEqual(t, stable, snapshot, "independent generated wildcard rows evolve")
				}
			}
			if spelling == handlerFixtureModule+"/archive.Record" {
				runtimeSource := strings.Replace(factoryTypedCurrentRuntime, " \"github.com/viant/datly/handlerfixture/shared\"\n", "", 1)
				runtimeSource = strings.ReplaceAll(runtimeSource, "shared.Record", "Record")
				runtimeSource = strings.Replace(runtimeSource, "require.Len(t,component.Views,1)", "require.Len(t,component.Views,3)", 1)
				runtimeSource = strings.Replace(runtimeSource, "require.Len(t,artifact.ViewDependencies,1)", "require.Len(t,artifact.ViewDependencies,3)", 1)
				runtimeSource = strings.Replace(runtimeSource, `"CREATE TABLE RECORDS(ID INTEGER NOT NULL,VALUE TEXT NOT NULL)"`, `"CREATE TABLE RECORDS(ID INTEGER NOT NULL,VALUE TEXT NOT NULL)","CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT,EXTRA TEXT)","CREATE TABLE CANDIDATES(ID INTEGER NOT NULL,NOTE TEXT)"`, 1)
				runtimeSource = strings.Replace(runtimeSource, " checked:=false", ` caller:=[]*Record{{Id:19,Value:"caller-owned",Has:&RecordHas{Value:true}}}
 checked:=false`, 1)
				start := strings.Index(runtimeSource, "  require.Len(t,in.InpData,1);")
				end := strings.Index(runtimeSource, "  checked=true;")
				require.Greater(t, start, 0)
				require.Greater(t, end, start)
				runtimeSource = runtimeSource[:start] + `  require.Len(t,in.InpData,1);require.NotNil(t,in.InpData[0].Has);require.True(t,in.InpData[0].Has.Target);require.False(t,in.InpData[0].Has.Exclusion)
  require.Len(t,in.Caller,1);require.Same(t,caller[0],in.Caller[0],"injected caller binding retains the authored local row pointer");require.Equal(t,19,in.Caller[0].Id);require.Equal(t,"caller-owned",in.Caller[0].Value);require.Same(t,caller[0].Has,in.Caller[0].Has);require.True(t,in.Caller[0].Has.Value)
` + runtimeSource[end:]
				runtimeSource = strings.Replace(runtimeSource, `strings.NewReader("{\"data\":[{\"children\":[{\"id\":0}]}]}")`, `strings.NewReader("{\"data\":[{\"target\":\"request\"}]}")`, 1)
				runtimeSource = strings.Replace(runtimeSource, "Providers:scope.Providers()", `Providers:scope.Providers(),Input:&ArchiveInput{Caller:caller,InpData:[]*Res{{Target:"request",Has:&ResHas{Target:true}}},Has:&ArchiveInputHas{Caller:true,InpData:true}}`, 1)
				writeSourceHandlerFile(t, root, "archive/current_runtime_test.go", runtimeSource)
				cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=2m", "./archive")
				cmd.Dir = root
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
			}
		})
	}
}

func TestFactoryBodyLocalCurrentRejectsCompetingAuthority(t *testing.T) {
	for _, tc := range []struct{ name, expression, options, row, want string }{
		{name: "missing", expression: "[]*archive.Missing", want: "requires selected authored ownership"},
		{name: "generated independent row", row: "// Code generated by Datly for Previous; DO NOT EDIT.\n" + factoryLocalRecord, want: "generated, not an authored borrowed row"},
		{name: "ambiguous authored row", row: factoryLocalRecord + "type Record struct{}\n", want: "declares type \"Record\" more than once"},
		{name: "view mismatch", options: ".TypeName('archive.Res')", want: "does not match package type"},
		{name: "body exclusion preserves mismatch", expression: "[]*archive.Res", options: ".TypeName('archive.Record')", want: "does not match package type"},
		{name: "output mismatch", expression: "[]*archive.Record,[]*archive.Res", want: "conflicting type"},
		{name: "output wrapper mismatch", expression: "[]*archive.Record,*archive.Record", want: "conflicting type wrappers"},
		{name: "destination", options: ".Dest('record.go')", want: "destination"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expression := tc.expression
			if expression == "" {
				expression = "[]*archive.Record"
			}
			root, source, _ := localCurrentBodyFixture(t, expression, tc.options)
			if tc.row != "" {
				writeSourceHandlerFile(t, root, "archive/record.go", tc.row)
			}
			before := sourceHandlerSnapshot(t, root)
			_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func TestFactoryLinkedCurrentCanonicalProjectionAuthority(t *testing.T) {
	const local, foreign = "example.com/app/local", "example.com/app/foreign"
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(struct{ Value string }{}), x.WithName("Record"), x.WithPkgPath(local))))
	require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeOf(struct{ Competing int }{}), x.WithName("Record"), x.WithPkgPath(foreign))))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{PackagePath: foreign})
	require.NoError(t, err)
	bare, err := resolver.ResolveShape("Record")
	require.NoError(t, err)
	require.Equal(t, foreign+".Record", bare.Descriptor.Key(), "bare lookup is resolvable but has competing authority")
	for _, tc := range []struct{ name, key, want string }{
		{name: "linked canonical selection", key: local + ".Record"},
		{name: "missing canonical cannot fall back", key: "example.com/app/missing.Record", want: "was not resolved"},
		{name: "empty canonical cannot fall back", want: "requires a canonical descriptor key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			generated := &gen.Plan{Views: []gen.ViewPlan{{Name: "Record", Type: "Record", Ownership: gen.ViewLinked, DescriptorKey: tc.key}}}
			generation := newHandlerGeneration(nil, &gen.Input{TargetPackage: local, TypeResolver: resolver}, Options{})
			fields, err := generation.currentProjectionFields(generated, "[]*Record", spec.CardinalityMany)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				return
			}
			require.NoError(t, err)
			require.Len(t, fields, 1)
			require.Equal(t, "Value", fields[0].name)
			require.Equal(t, "string", fields[0].expression)
		})
	}
}
