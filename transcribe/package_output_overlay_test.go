package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/testdata/outputoverlay"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	legacyresponse "github.com/viant/xdatly/handler/response"
)

const overlayFixturePackage = "github.com/viant/datly/transcribe/testdata/outputoverlay"
const outputOverlayDQL = `#package('example.com/generated/reader')
#import('legacy','github.com/viant/datly/transcribe/testdata/outputoverlay')
#import('response','github.com/viant/xdatly/handler/response')

#setting($_ = $route('/overlay-records','GET'))
#setting($_ = $output_type('legacy.Output'))
#setting($_ = $cube())
#setting($_ = $cubeCompose(true))

#define($_ = $ID<int>(query/id).Optional().WithPredicate(0,'equal','r','ID'))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(Records).Optional())
#define($_ = $Status<response.Status>(output/status).WithTag('parameter:"Status,kind=output,in=status" json:",omitempty"'))
#define($_ = $Data<[]*legacy.Row>(output/view))

SELECT r.*,type(r,'legacy.Row'),grouping_enabled(r),set_limit(r,3),use_connector(r,'current')
FROM (SELECT ID,NAME FROM records) r
`

func compileOutputOverlay(t *testing.T, text string, catalog *typecatalog.Catalog) (*Result, error) {
	t.Helper()
	found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[outputoverlay.Component]().PkgPath()}, nil)
	require.NoError(t, err)
	require.Len(t, found.Components, 1)
	if catalog == nil {
		catalog = typecatalog.NewCatalog()
	}
	require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[legacyresponse.Status]()), x.NewType(reflect.TypeFor[outputoverlay.Row]()), x.NewType(reflect.TypeFor[outputoverlay.Metric]())))
	resources := resource.New()
	require.NoError(t, resources.Register("", outputoverlay.Resources))
	return (&PackageCompilation{Source: &Source{Scope: overlayFixturePackage, Name: "Records", Text: text, Resources: resources}, Component: found.Components[0], InputType: reflect.TypeFor[outputoverlay.Input](), OutputType: reflect.TypeFor[outputoverlay.Output](), Types: catalog}).Compile(context.Background())
}
func TestPackageOverlayHonorsExplicitQualifiedOutput(t *testing.T) {
	result, err := compileOutputOverlay(t, outputOverlayDQL, nil)
	require.NoError(t, err)
	require.NotNil(t, result.Contracts.Input)
	require.NotNil(t, result.Contracts.Output)
	require.Equal(t, overlayFixturePackage+".Output", result.Contracts.Output.DescriptorKey)
	require.Equal(t, overlayFixturePackage+".Row", result.Views[gen.RootViewPath].DescriptorKey)
	require.NotNil(t, result.Component.SelectorCompanion)
	require.NotNil(t, result.Component.RootView.Source.Controls.Limit)
	require.Equal(t, 3, *result.Component.RootView.Source.Controls.Limit)
	require.Equal(t, "current", result.Component.RootView.Source.Bindings.Connector)
	f, _ := reflect.TypeFor[outputoverlay.Output]().FieldByName("Status")
	require.True(t, f.Anonymous)
	_, present := reflect.TypeFor[outputoverlay.Input]().FieldByName("Fields")
	require.False(t, present)
	_, present = reflect.TypeFor[outputoverlay.InputHas]().FieldByName("Fields")
	require.False(t, present)
}
func TestPackageOverlayKeepsImplicitOutputRules(t *testing.T) {
	for _, tc := range []struct{ name, replace string }{{"omitted", ""}, {"unqualified", "#setting($_ = $output_type('Output'))"}} {
		t.Run(tc.name, func(t *testing.T) {
			text := strings.Replace(outputOverlayDQL, "#setting($_ = $output_type('legacy.Output'))", tc.replace, 1)
			result, err := compileOutputOverlay(t, text, nil)
			require.NoError(t, err)
			require.NotNil(t, result.Contracts.Input)
			require.Nil(t, result.Contracts.Output)
		})
	}
}
func TestPackageOverlayRejectsExplicitOutputTypeChanges(t *testing.T) {
	for _, tc := range []struct{ name, old, new, want string }{
		{"collection", "$Data<[]*legacy.Row>", "$Data<*legacy.Row>", "differs from linked field"},
		{"row pointer", "$Data<[]*legacy.Row>", "$Data<[]legacy.Row>", "differs from linked field"},
		{"status package", "github.com/viant/xdatly/handler/response", "github.com/viant/xdatly/response", "differs from linked field"},
		{"binding", "$Data<[]*legacy.Row>(output/view)", "$Data<[]*legacy.Row>(output/body)", "incompatible binding metadata"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileOutputOverlay(t, strings.Replace(outputOverlayDQL, tc.old, tc.new, 1), nil)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestPackageOverlayNativeGenerationReloadRetainsControls(t *testing.T) {
	testPackageOverlayNativeGenerationReload(t, false)
}
func TestReaderMetadataNativeNoSelectorDQLRefresh(t *testing.T) {
	testPackageOverlayNativeGenerationReload(t, true)
}
func testPackageOverlayNativeGenerationReload(t *testing.T, withoutSelector bool) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/transcribe/testdata"}).Write(t, root)
	for _, path := range []string{"fixture.go", "holder.go", "sql/records.sql", "sql/metric.sql"} {
		data, err := os.ReadFile(filepath.Join("testdata/outputoverlay", path))
		require.NoError(t, err)
		file := filepath.Join(root, "outputoverlay", path)
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
		require.NoError(t, os.WriteFile(file, data, 0600))
	}
	text := strings.Replace(outputOverlayDQL, "#package('example.com/generated/reader')", "#package('outputoverlay')", 1)
	text = strings.Replace(text, "#setting($_ = $cubeCompose(true))", "#setting($_ = $cubeCompose(true))\n#setting($_ = $cache(true,'5m'))\n#setting($_ = $cache_warmup('ID'))", 1)
	text = strings.Replace(text, "SELECT r.*,type(r,'legacy.Row'),grouping_enabled(r),set_limit(r,3),use_connector(r,'current')\nFROM (SELECT ID,NAME FROM records) r", "SELECT r.*,metric.*,type(r,'legacy.Row'),type(metric,'legacy.Metric'),cardinality(metric,'one'),grouping_enabled(r),set_limit(r,3),use_connector(r,'current'),use_cache(r,'root_cache'),set_limit(metric,2),use_connector(metric,'nested'),use_cache(metric,'nested_cache')\nFROM (SELECT ID,NAME FROM records) r\nLEFT JOIN (SELECT ID,AMOUNT FROM metrics) metric ON metric.ID=r.ID", 1)

	if withoutSelector {
		text = strings.Replace(text, "#set($_ = $Fields<[]string>(query/fields).QuerySelector(Records).Optional())", "", 1)
	}
	result, err := compileOutputOverlay(t, text, nil)
	require.NoError(t, err)
	if withoutSelector {
		// This variant selects a plain canonical reader fixture: original public
		// Input/Output have no selector field, and the initial discovery holder's
		// optional selector-only declaration is outside this fixture's query graph.
		result.Component.SelectorCompanion = nil
		result.Component.RootView.Selector = nil
		var parameters []*spec.Parameter
		for _, parameter := range result.Component.Parameters {
			if parameter.QuerySelector == nil {
				parameters = append(parameters, parameter)
			}
		}
		result.Component.Parameters = parameters
	}
	generated, err := (Generator{Operation: "get", Language: HandlerGo}).Generate(context.Background(), GenerationRequest{Compiled: result, Destination: root})
	require.NoError(t, err)
	require.Equal(t, gen.ContractLinked, generated.Result.Plan.Input.Ownership)
	require.Equal(t, gen.ContractLinked, generated.Result.Plan.Output.Ownership)
	require.NotEmpty(t, generated.Result.Plan.Cubes, "native cube must actually be emitted")
	// Retire only the temporary authored discovery holder. Public contracts and
	// SQL stay byte-identical; attach the same authored metadata to the one native holder.
	require.NoError(t, os.Remove(filepath.Join(root, "outputoverlay/holder.go")))
	const companion = `package outputoverlay
 func(RecordsComponent)DatlyQuerySelectors()selectors{panic("must not execute")}
 `
	if !withoutSelector {
		require.NoError(t, os.WriteFile(filepath.Join(root, "outputoverlay/selectors.go"), []byte(companion), 0600))
	}
	fixture := `package outputoverlay
 import("reflect";"testing";"os";"path/filepath";"strings";"encoding/json";"github.com/viant/datly/internal/readergraph";"github.com/viant/datly/bootstrap";"github.com/viant/bindly/resource")
 func TestReload(t *testing.T){
 found,err:=bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[RecordsComponent]().PkgPath()},nil);if err!=nil{t.Fatal(err)}
 var checked int
 for _,source:=range found.Components{if source.Tag.Name!="Records"{continue};checked++;component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 if component.SelectorCompanion==nil{t.Fatal("selector association lost")}
 status,_:=reflect.TypeFor[Output]().FieldByName("Status");if !status.Anonymous||status.Type.PkgPath()!="github.com/viant/xdatly/handler/response"{t.Fatal("legacy anonymous status changed")}
 data,_:=reflect.TypeFor[Output]().FieldByName("Data");if data.Type!=reflect.TypeFor[[]*Row](){t.Fatal("row collection changed")}
 metric,_:=reflect.TypeFor[Row]().FieldByName("Metric");if metric.Type!=reflect.TypeFor[*Metric](){t.Fatal("nested pointer changed")}
 amount,_:=reflect.TypeFor[Metric]().FieldByName("Amount");if amount.Type!=reflect.TypeFor[*float64](){t.Fatal("scalar pointer changed")}
 if _,ok:=reflect.TypeFor[Input]().FieldByName("Fields");ok{t.Fatal("public selector field added")}
 if _,ok:=reflect.TypeFor[InputHas]().FieldByName("Fields");ok{t.Fatal("public selector marker added")}
 var selector bool;for _,p:=range component.Parameters{if p.Name=="Fields"&&p.QuerySelector!=nil{if p.QuerySelector.View!="Records"||p.QuerySelector.Property!="fields"{t.Fatal("selector target changed")};selector=true}};if !selector{t.Fatal("selector declaration lost")}
 if component.RootView.Source.Controls==nil||component.RootView.Source.Controls.Limit==nil||*component.RootView.Source.Controls.Limit!=3{t.Fatalf("DQL SQL limit lost on native Go-only reload: %+v",component.RootView.Source.Controls)}
 if component.RootView.Source.Bindings==nil||component.RootView.Source.Bindings.Connector!="current"{t.Fatal("connector lost")}
 if component.Settings.Report==nil||!component.Settings.Report.Enabled{t.Fatal("cube policy lost")}
 resources:=resource.New();holder:=RecordsComponent{};if err:=resources.Register(holder.EmbedNamespace(),holder.EmbedFS());err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)};if artifact.Reader==nil{t.Fatal("native reader compilation absent")}
 directory:=os.Getenv("NATIVE_READER_SOURCE_DIR");namespace,metadataPath,_:=strings.Cut(source.Tag.ReaderMetadata,":");filename:=filepath.Join(directory,metadataPath);original,err:=os.ReadFile(filename);if err!=nil{t.Fatal(err)}
 document,err:=readergraph.StrictDecode(original);if err!=nil{t.Fatal(err)};for _,relation:=range document.Component.RootView.Relations{identity,err:=relation.View.Identity();if err!=nil{t.Fatal(err)};delete(document.Rows,identity)};document.Component.RootView.Relations=nil
 body,err:=json.Marshal(document);if err!=nil{t.Fatal(err)};if err=os.WriteFile(filename,body,0600);err!=nil{t.Fatal(err)}
 copied:=*source;copied.Dir=directory;removed,err:=copied.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)};current:=resource.New();if err=current.Register(namespace,os.DirFS(directory));err!=nil{t.Fatal(err)}
 without,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:removed,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:current});if err!=nil{t.Fatal(err)};if len(without.Reader.Root.View.Relations)!=0{t.Fatal("stale original relation tag restored")};if err=os.WriteFile(filename,original,0600);err!=nil{t.Fatal(err)}


 if component.RootView.Source.Bindings.CacheName!="root_cache"{t.Fatal("root cache binding lost")}
 if component.Settings.Cache==nil||component.Settings.Cache.Warmup==nil{t.Fatal("cache warmup policy lost")}
 var predicate bool;for _,p:=range component.Parameters{if p.Name=="ID"&&len(p.Predicates)>0{predicate=true}};if !predicate{t.Fatal("predicate declaration lost")}
 var child bool;for _,r:=range component.RootView.Relations{if r.Holder!="Metric"{continue};child=true;if len(r.On)!=1{t.Fatal("relation links lost")};if r.View.Source.Controls==nil||r.View.Source.Controls.Limit==nil||*r.View.Source.Controls.Limit!=2{t.Fatal("nested limit lost")};if r.View.Source.Bindings==nil||r.View.Source.Bindings.Connector!="nested"||r.View.Source.Bindings.CacheName!="nested_cache"{t.Fatal("nested bindings lost")}};if !child{t.Fatal("nested relation lost")}

 }
 if checked!=1{t.Fatalf("native holder count %d",checked)}
 }
 `
	if withoutSelector {
		fixture = strings.Replace(fixture, `if component.SelectorCompanion==nil{t.Fatal("selector association lost")}`, `if component.SelectorCompanion!=nil{t.Fatal("unexpected selector association")}`, 1)
		fixture = strings.Replace(fixture, `if !selector{t.Fatal("selector declaration lost")}`, `if selector{t.Fatal("unexpected selector declaration")}`, 1)
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "outputoverlay/reload_test.go"), []byte(fixture), 0600))
	// Compile the exact nominal package through a source overlay: a nested module
	// would ambiguously duplicate this fixture in the required parent SDK module.
	sdkRoot, err := filepath.Abs("..")
	require.NoError(t, err)
	replacements := map[string]string{}
	require.NoError(t, filepath.WalkDir(filepath.Join(root, "outputoverlay"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(filepath.Join(root, "outputoverlay"), path)
		if err != nil {
			return err
		}
		replacements[filepath.Join(sdkRoot, "transcribe/testdata/outputoverlay", rel)] = path
		return nil
	}))
	emptyHolder := filepath.Join(root, "retired_holder.go")
	require.NoError(t, os.WriteFile(emptyHolder, []byte("package outputoverlay\n"), 0600))
	replacements[filepath.Join(sdkRoot, "transcribe/testdata/outputoverlay/holder.go")] = emptyHolder
	overlay, err := json.Marshal(map[string]any{"Replace": replacements})
	require.NoError(t, err)
	overlayPath := filepath.Join(root, "native-overlay.json")
	require.NoError(t, os.WriteFile(overlayPath, overlay, 0600))
	command := exec.Command("go", "test", "-overlay", overlayPath, "./transcribe/testdata/outputoverlay")
	command.Dir = sdkRoot
	command.Env = append(os.Environ(), "GOWORK=off", "NATIVE_READER_SOURCE_DIR="+filepath.Join(root, "outputoverlay"))
	checkNativeReaderSourceDescriptors(t, root, !withoutSelector)
	runNative := func() ([]byte, error) {
		fresh := exec.Command(command.Path, command.Args[1:]...)
		fresh.Dir = command.Dir
		fresh.Env = command.Env
		return fresh.CombinedOutput()
	}
	first, err := runNative()
	require.NoError(t, err, "first native reload/compile: %s", first)
	for _, path := range []string{"fixture.go", "sql/records.sql", "sql/metric.sql"} {
		original, err := os.ReadFile(filepath.Join("testdata/outputoverlay", path))
		require.NoError(t, err)
		actual, err := os.ReadFile(filepath.Join(root, "outputoverlay", path))
		require.NoError(t, err)
		require.Equal(t, string(original), string(actual), "authored bytes changed: %s", path)
	}

	before := map[string]string{}
	for _, file := range generated.Result.Files {
		data, err := os.ReadFile(file.Path)
		require.NoError(t, err)
		before[file.Path] = string(data)
	}
	const nativeAuthor = `package main
import("context";"encoding/json";"os";"reflect";"strings";"github.com/viant/bindly/resource";"github.com/viant/datly/bootstrap";"github.com/viant/datly/transcribe";"github.com/viant/datly/transcribe/testdata/outputoverlay")
func main(){_,_=reflect.TypeFor[outputoverlay.RecordsComponent]().MethodByName("DatlyQuerySelectors");pkg:=reflect.TypeFor[outputoverlay.RecordsComponent]().PkgPath();found,err:=bootstrap.ReflectSelectedPackages([]string{pkg},nil);if err!=nil{panic(err)};for _,route:=range found.Components{if route.Tag.Name!="Records"{continue};admitted,err:=route.Resolve(reflect.TypeFor[outputoverlay.Input](),reflect.TypeFor[outputoverlay.Output]());if err!=nil{panic(err)};before,_:=json.Marshal(admitted);marker:=admitted.ReaderGraph;holder:=outputoverlay.RecordsComponent{};resources:=resource.New();if err=resources.Register("",outputoverlay.Resources);err!=nil{panic(err)};if err=resources.Register(holder.EmbedNamespace(),holder.EmbedFS());err!=nil{panic(err)};text,err:=os.ReadFile(os.Getenv("NATIVE_DQL"));if err!=nil{panic(err)};compilation:=&transcribe.PackageCompilation{Source:&transcribe.Source{Scope:pkg,Name:"Records",Text:string(text),Resources:resources},Component:route,InputType:reflect.TypeFor[outputoverlay.Input](),OutputType:reflect.TypeFor[outputoverlay.Output](),Types:found.Types};result,err:=compilation.Compile(context.Background());if err!=nil{panic(err)};after,_:=json.Marshal(admitted);if string(before)!=string(after)||admitted.ReaderGraph!=marker{panic("admitted authority mutated")};if _,err=(transcribe.Generator{Operation:"get",Language:transcribe.HandlerGo}).Generate(context.Background(),transcribe.GenerationRequest{Compiled:result,Destination:os.Getenv("NATIVE_DEST")});err!=nil{panic(err)};compilation.Source.Text="SELECT (";if _,err=compilation.Compile(context.Background());err==nil{panic("invalid source accepted")};after,_=json.Marshal(admitted);if string(before)!=string(after)||admitted.ReaderGraph!=marker{panic("failed compile mutated authority")};return};panic(strings.Join(found.Packages,","))}
`
	authorPath := filepath.Join(root, "native-author.go")
	require.NoError(t, os.WriteFile(authorPath, []byte(nativeAuthor), 0600))
	dqlPath := filepath.Join(root, "records.dql")
	require.NoError(t, os.WriteFile(dqlPath, []byte(text), 0600))
	authorCommand := exec.Command("go", "run", "-overlay", overlayPath, authorPath)
	authorCommand.Dir = sdkRoot
	authorCommand.Env = append(command.Env, "NATIVE_DQL="+dqlPath, "NATIVE_DEST="+root)
	authorOutput, err := authorCommand.CombinedOutput()
	require.NoError(t, err, "rediscovered native holder DQL authoring: %s", authorOutput)
	for path, want := range before {
		actual, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, want, string(actual), path)
	}
	checkNativeReaderSourceDescriptors(t, root, !withoutSelector)
	output, err := runNative()
	require.NoError(t, err, "repeat native generated reload: %s", output)
}

func TestPackageOverlayRetainsValidatedRootReceipt(t *testing.T) {
	output := x.NewType(reflect.TypeFor[outputoverlay.Output]())
	row := x.NewType(reflect.TypeFor[outputoverlay.Row]())
	receipt := &gen.BorrowedRowAuthority{OwnerName: "original-authority"}
	prior := &gen.ViewReference{DescriptorKey: row.Key(), Borrowed: receipt}
	result := &Result{Contracts: gen.ContractReferences{Output: &gen.ContractReference{DescriptorKey: output.Key()}}, Views: gen.ViewReferences{gen.RootViewPath: prior}}
	require.NoError(t, retainPackageOutputRoot(result, output, row))
	require.Same(t, prior, result.Views[gen.RootViewPath])
	require.Same(t, receipt, result.Views[gen.RootViewPath].Borrowed)
	other := x.NewType(reflect.TypeFor[outputoverlay.Metric]())
	require.ErrorContains(t, retainPackageOutputRoot(result, output, other), "conflicting root")
	require.Same(t, prior, result.Views[gen.RootViewPath])
	// Explicit alternate output cannot acquire this package's old row authority.
	result.Contracts.Output.DescriptorKey = "example.com/alternate.Output"
	result.Views = nil
	require.NoError(t, retainPackageOutputRoot(result, output, row))
	require.Empty(t, result.Views)
}

func checkNativeReaderSourceDescriptors(t *testing.T, root string, withSelector bool) {
	t.Helper()
	directory := filepath.Join(root, "outputoverlay")
	names, err := filepath.Glob(filepath.Join(directory, "*.go"))
	require.NoError(t, err)
	var files []xmodule.File
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, xmodule.File{Path: name, Dir: directory, ImportPath: overlayFixturePackage})
	}
	routes, err := (bootstrap.PackageDiscovery{}).DiscoverFiles(files)
	require.NoError(t, err)
	loaded, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "outputoverlay")
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	for _, typ := range loaded.Types {
		require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, &x.Type{Name: typ.Name, PkgPath: loaded.PkgPath, SynteticType: typ}))
	}
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[legacyresponse.Status]())))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	require.NoError(t, err)
	var selected []*bootstrap.RouteSource
	for _, route := range routes {
		if route.Tag.Name == "Records" {
			selected = append(selected, route)
		}
	}
	require.Len(t, selected, 1)
	require.Nil(t, selected[0].LinkedInputType, "source-only proof must not borrow reflected native holder")
	groups, err := bootstrap.GroupPackageComponentSources(selected, resolver)
	require.NoError(t, err)
	require.Len(t, groups, 1)
	component, err := groups[0].ResolveDescriptors(resolver)
	require.NoError(t, err)
	require.NotNil(t, component.ReaderGraph)
	if withSelector {
		require.NotNil(t, component.SelectorCompanion)
	} else {
		require.Nil(t, component.SelectorCompanion)
	}
	require.Equal(t, 3, *component.RootView.Source.Controls.Limit)
	require.Equal(t, "current", component.RootView.Source.Bindings.Connector)
	require.Equal(t, "root_cache", component.RootView.Source.Bindings.CacheName)
	namespace, metadataPath, ok := strings.Cut(selected[0].Tag.ReaderMetadata, ":")
	require.True(t, ok)
	filename := filepath.Join(directory, metadataPath)
	original, err := os.ReadFile(filename)
	require.NoError(t, err)
	resolve := func(route *bootstrap.RouteSource) (*spec.Component, error) {
		groups, err := bootstrap.GroupPackageComponentSources([]*bootstrap.RouteSource{route}, resolver)
		if err != nil {
			return nil, err
		}
		return groups[0].ResolveDescriptors(resolver)
	}
	for _, tc := range []struct {
		name  string
		alter func(*readergraph.Document)
	}{
		{"foreign input", func(d *readergraph.Document) { d.Input = "example.com/foreign.Input" }},
		{"foreign output", func(d *readergraph.Document) { d.Output = "example.com/foreign.Output" }},
		{"foreign holder", func(d *readergraph.Document) { d.Holder = "ForeignComponent" }},
		{"foreign field", func(d *readergraph.Document) { d.Field = "Foreign" }},
		{"foreign package", func(d *readergraph.Document) { d.Package = "example.com/foreign" }},
		{"foreign route", func(d *readergraph.Document) { d.Component.Routes[0].Path = "/foreign" }},
		{"wrong root row", func(d *readergraph.Document) { d.Component.RootView.TypeName = overlayFixturePackage + ".Metric" }},
		{"absent columns", func(d *readergraph.Document) { d.Component.RootView.Columns = nil }},
		{"absent output binding", func(d *readergraph.Document) {
			var params []*spec.Parameter
			for _, p := range d.Component.Parameters {
				if p.Source.Kind == "output" && p.Source.Name == "view" {
					continue
				}
				params = append(params, p)
			}
			d.Component.Parameters = params
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			document, err := readergraph.StrictDecode(original)
			require.NoError(t, err)
			tc.alter(document)
			body, err := json.Marshal(document)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filename, body, 0600))
			defer os.WriteFile(filename, original, 0600)
			_, err = resolve(selected[0])
			require.Error(t, err)
		})
	}
	for _, uri := range []string{namespace + ":../escape.json", "foreign:" + metadataPath, namespace + ":metadata/missing.json"} {
		copy := *selected[0]
		copy.Tag.ReaderMetadata = uri
		_, err := resolve(&copy)
		require.Error(t, err)
	}
	// Full absence is authoritative: removing a canonical relation and its row
	// membership must not resurrect original Go relation tags during compilation.
	document, err := readergraph.StrictDecode(original)
	require.NoError(t, err)
	for _, relation := range document.Component.RootView.Relations {
		identity, err := relation.View.Identity()
		require.NoError(t, err)
		delete(document.Rows, identity)
	}
	document.Component.RootView.Relations = nil
	body, err := json.Marshal(document)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filename, body, 0600))
	defer os.WriteFile(filename, original, 0600)
	removed, err := resolve(selected[0])
	require.NoError(t, err)
	require.Empty(t, removed.RootView.Relations)
	require.NoError(t, os.WriteFile(filename, original, 0600))

}
