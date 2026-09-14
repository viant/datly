package bootstrap

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
)

type ColumnBase struct {
	Region string `sqlx:"region_code" groupable:"true"`
}
type ColumnHidden struct{ Secret string }
type ColumnRow struct {
	ColumnBase
	ColumnHidden `sqlx:"-"`
	ID           *int               `sqlx:"account_id,primaryKey=true,autoincrement=true" groupable:"true"`
	Amount       float64            `sqlx:"total_spend,type=DECIMAL" groupable:"false"`
	Created      *time.Time         `sqlx:"created_at"`
	Label        string             `source:"display_label" codec:"label,outputType=string,prefix"`
	Hidden       string             `sqlx:"-"`
	JSONHidden   string             `json:"-"`
	Internal     string             `internal:"true"`
	Has          *struct{ ID bool } `setMarker:"true"`
	Children     []*ColumnRow       `on:"ID=ID"`
	Parent       *ColumnRow         `view:"Parent,table=parents"`
	private      string
}
type ColumnOutput struct {
	Data []*ColumnRow `parameter:"result,kind=output,in=view" view:"Report,groupable=true" sql:"SELECT account_id,total_spend,region_code,created_at,display_label FROM records"`
}

const columnContractSource = `package columns
import "time"
type ColumnBase struct { Region string ` + "`sqlx:\"region_code\" groupable:\"true\"`" + ` }
type ColumnHidden struct { Secret string }
type ColumnRow struct {
 ColumnBase
 ColumnHidden ` + "`sqlx:\"-\"`" + `
 ID *int ` + "`sqlx:\"account_id,primaryKey=true,autoincrement=true\" groupable:\"true\"`" + `
 Amount float64 ` + "`sqlx:\"total_spend,type=DECIMAL\" groupable:\"false\"`" + `
 Created *time.Time ` + "`sqlx:\"created_at\"`" + `
 Label string ` + "`source:\"display_label\" codec:\"label,outputType=string,prefix\"`" + `
 Hidden string ` + "`sqlx:\"-\"`" + `
 JSONHidden string ` + "`json:\"-\"`" + `
 Internal string ` + "`internal:\"true\"`" + `
 Has *struct { ID bool } ` + "`setMarker:\"true\"`" + `
 Children []*ColumnRow ` + "`on:\"ID=ID\"`" + `
 Parent *ColumnRow ` + "`view:\"Parent,table=parents\"`" + `
 private string
}
type InlineOutput struct { Data []struct { ID *int ` + "`sqlx:\"inline_id\"`" + ` } ` + "`parameter:\"view,kind=output,in=view\" view:\"Inline,table=records\"`" + ` }
type ColumnOutput struct { Data []*ColumnRow ` + "`parameter:\"result,kind=output,in=view\" view:\"Report,groupable=true\" sql:\"SELECT account_id,total_spend,region_code,created_at,display_label FROM records\"`" + ` }
`

func TestPackageOutputCanonicalColumns(t *testing.T) {
	root := t.TempDir()
	writeFile(t, root, "go.mod", testGoMod)
	writeFile(t, root, "columns/contract.go", columnContractSource)
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "columns")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/app/columns"})
	if err != nil {
		t.Fatal(err)
	}
	synthetic, err := resolver.Descriptor("example.com/app/columns.ColumnOutput")
	if err != nil {
		t.Fatal(err)
	}
	inline, err := resolver.Descriptor("example.com/app/columns.InlineOutput")
	if err != nil {
		t.Fatal(err)
	}
	inlineComponent, err := (ContractResolver{OutputType: inline, Types: resolver}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, (&outputColumnCompiler{output: xshape.New(inline, resolver.Descriptor)}).compile(inlineComponent.RootView, "Data"))
	require.Equal(t, []*spec.Column{{Name: "ID", NameInferred: true, Source: "inline_id", Type: spec.TypeRef{Name: "int", Pointer: true}, Nullable: true, Groupable: new(bool), Tag: `sqlx:"inline_id"`}}, inlineComponent.RootView.Columns)
	for _, tc := range []struct {
		name   string
		output *x.Type
		types  *typecatalog.Resolver
	}{{"linked", xshape.Linked(reflect.TypeOf(ColumnOutput{})).Descriptor(), nil}, {"synthetic", synthetic, resolver}} {
		t.Run(tc.name, func(t *testing.T) {
			for _, existing := range []bool{false, true} {
				t.Run(map[bool]string{false: "new-view", true: "existing-source"}[existing], func(t *testing.T) {
					base := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/shared", Name: "Report"}}
					if existing {
						base.RootView = &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/shared", Name: "Report"}, Name: "Report", Source: &spec.ViewSource{SQL: "SELECT account_id FROM records"}}
					}
					component, err := (ContractResolver{Component: base, OutputType: tc.output, Types: tc.types}).Resolve()
					if err != nil {
						t.Fatal(err)
					}
					lookup := xshape.Lookup(nil)
					if tc.types != nil {
						lookup = tc.types.Descriptor
					}
					require.NoError(t, (&outputColumnCompiler{output: xshape.New(tc.output, lookup)}).compile(component.RootView, "Data"))
					columns := component.RootView.Columns
					if len(columns) != 5 {
						t.Fatalf("scalar columns=%+v", columns)
					}
					wantNames := []string{"Region", "ID", "Amount", "Created", "Label"}
					wantSources := []string{"region_code", "account_id", "total_spend", "created_at", "display_label"}
					for i, c := range columns {
						if c.Name != wantNames[i] || c.Source != wantSources[i] {
							t.Fatalf("column %d=%+v", i, c)
						}
					}
					if component.RootView.Key.Scope != "example.com/shared" {
						t.Fatalf("view scope drift=%s", component.RootView.Key.Scope)
					}
					if columns[1].Type != (spec.TypeRef{Name: "int", Pointer: true}) || !columns[1].Nullable || !columns[1].PrimaryKey || !columns[1].AutoIncrement || !*columns[1].Groupable {
						t.Fatalf("ID=%+v", columns[1])
					}
					if columns[2].Type.Name != "float64" || columns[2].DatabaseType != "DECIMAL" || columns[2].Groupable == nil || *columns[2].Groupable {
						t.Fatalf("measure=%+v", columns[2])
					}
					if columns[3].Type != (spec.TypeRef{Package: "time", Name: "Time", Pointer: true}) {
						t.Fatalf("qualified pointer=%+v", columns[3])
					}
					if !reflect.DeepEqual(columns[4].Codec, &spec.Codec{Body: "label", OutputType: "string", Args: []string{"prefix"}}) {
						t.Fatalf("codec=%+v", columns[4])
					}
					if base.RootView != nil && base.RootView.Columns != nil {
						t.Fatal("source metadata mutated")
					}
					again, err := (ContractResolver{Component: component, OutputType: tc.output, Types: tc.types}).Resolve()
					if err != nil {
						t.Fatal(err)
					}
					require.Equal(t, component.RootView, again.RootView, "repeat view resolution")
				})
			}
		})
	}
}

func TestPackageOutputPreservesCanonicalProjection(t *testing.T) {
	flag := false
	for _, source := range []bool{false, true} {
		for _, columns := range [][]*spec.Column{{}, {{Name: "Authored", Source: "account_id", Expression: "COUNT(*)", Type: spec.TypeRef{Name: "int64"}, ExplicitType: true, Groupable: &flag, Codec: &spec.Codec{Body: "authored", Args: []string{"arg"}}}}} {
			base := &spec.Component{RootView: &spec.View{Name: "Report", Columns: columns}}
			if source {
				base.RootView.Source = &spec.ViewSource{SQL: "SELECT account_id FROM records"}
			}
			actual, err := (ContractResolver{Component: base, OutputType: xshape.Linked(reflect.TypeOf(ColumnOutput{})).Descriptor()}).Resolve()
			if err != nil {
				t.Fatal(err)
			}
			require.NoError(t, (&outputColumnCompiler{output: xshape.Linked(reflect.TypeOf(ColumnOutput{}))}).compile(actual.RootView, "Data"))
			if !reflect.DeepEqual(actual.RootView.Columns, columns) {
				t.Fatalf("canonical projection changed: %+v", actual.RootView.Columns)
			}
			if len(columns) > 0 {
				actual.RootView.Columns[0].Codec.Args[0] = "changed"
				if columns[0].Codec.Args[0] != "arg" {
					t.Fatal("canonical clone aliases input")
				}
			}
		}
	}
}

func TestPackageOutputExcludesCanonicalRelationHolder(t *testing.T) {
	type row struct {
		ID       int
		Children []*ColumnRow
	}
	type output struct {
		Data []*row `parameter:"view,kind=output,in=view"`
	}
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: &spec.View{Name: "Children"}}}}
	component, err := (ContractResolver{Component: &spec.Component{RootView: view}, OutputType: xshape.Linked(reflect.TypeOf(output{})).Descriptor()}).Resolve()
	require.NoError(t, err)
	require.NoError(t, (&outputColumnCompiler{output: xshape.Linked(reflect.TypeOf(output{}))}).compile(component.RootView, "Data"))
	require.Len(t, component.RootView.Columns, 1)
	require.Equal(t, "ID", component.RootView.Columns[0].Name)
	require.Nil(t, view.Columns)
}

func TestPackageOutputInvalidColumnMetadataIsAtomic(t *testing.T) {
	type row struct {
		ID     int
		Amount float64 `groupable:"sometimes"`
	}
	type output struct {
		Data []*row `parameter:"view,kind=output,in=view"`
	}
	original := &spec.Component{RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT id,amount FROM records"}}}
	result, err := (ContractResolver{Component: original, OutputType: xshape.Linked(reflect.TypeOf(output{})).Descriptor()}).Resolve()
	require.NoError(t, err)
	err = (&outputColumnCompiler{output: xshape.Linked(reflect.TypeOf(output{}))}).compile(result.RootView, "Data")
	require.ErrorContains(t, err, "groupable")
	require.Nil(t, result.RootView.Columns)
	require.Nil(t, original.RootView.Columns)
}

func TestArtifactCompilesCanonicalColumnsBeforeReaderPlan(t *testing.T) {
	type input struct{}
	type row struct {
		ID     *int    `sqlx:"account_id" groupable:"true"`
		Amount float64 `sqlx:"total_amount"`
	}
	type output struct {
		Data []*row `parameter:"view,kind=output,in=view" view:"Records,groupable=true" sql:"SELECT account_id,total_amount FROM records"`
	}
	declaration, err := (ContractResolver{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/shared", Name: "Records"}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}}, OutputType: xshape.Linked(reflect.TypeOf(output{})).Descriptor()}).Resolve()
	require.NoError(t, err)
	require.Nil(t, declaration.RootView.Columns, "declaration authority must stay stable for transcribe")
	artifact, err := BuildArtifact(ArtifactInput{Component: declaration, InputType: reflect.TypeOf(input{}), OutputType: reflect.TypeOf(output{})})
	require.NoError(t, err)
	require.Len(t, artifact.Component.RootView.Columns, 2)
	require.Equal(t, artifact.Component.RootView.Columns, artifact.Reader.Root.View.Spec.Columns)
	require.Equal(t, "ID", artifact.Component.RootView.Columns[0].Name)
	require.Equal(t, "account_id", artifact.Component.RootView.Columns[0].Source)
	require.Nil(t, declaration.RootView.Columns, "artifact compilation mutated source declaration")
}

const anonymousScalarSource = `package amounts

type BaseAmount float64
type Amount BaseAmount
type Dimensions struct { Region string ` + "`sqlx:\"region_code\" groupable:\"true\"`" + ` }
type Hidden struct { Secret string }
type ValueRow struct {
 Amount
 Dimensions
 Hidden ` + "`sqlx:\"-\"`" + `
}
type PointerRow struct {
 *Amount
 *Dimensions
 Hidden ` + "`sqlx:\"-\"`" + `
}
type ValueOutput struct { Data []ValueRow ` + "`parameter:\"view,kind=output,in=view\" view:\"Amounts,groupable=true\" sql:\"SELECT Amount,region_code FROM amounts\"`" + ` }
type PointerOutput struct { Data []*PointerRow ` + "`parameter:\"view,kind=output,in=view\" view:\"Amounts,groupable=true\" sql:\"SELECT Amount,region_code FROM amounts\"`" + ` }
`

// The same authored source is loaded as synthetic descriptors and compiled by
// Go for public ArtifactBuilder/report tests; neither branch supplies columns.
func TestPackageOutputAnonymousScalar(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/anonymousscalar"}).Write(t, root)
	writeFile(t, root, "amounts/contract.go", anonymousScalarSource)
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "amounts")
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg))
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/anonymousscalar/amounts"})
	require.NoError(t, err)
	for _, name := range []string{"ValueOutput", "PointerOutput"} {
		t.Run("synthetic/"+name, func(t *testing.T) {
			descriptor, err := resolver.Descriptor("example.com/anonymousscalar/amounts." + name)
			require.NoError(t, err)
			component, err := (ContractResolver{OutputType: descriptor, Types: resolver}).Resolve()
			require.NoError(t, err)
			require.Nil(t, component.RootView.Columns)
			compiler := &outputColumnCompiler{output: xshape.New(descriptor, resolver.Descriptor), lookup: resolver.Descriptor}
			require.NoError(t, compiler.compile(component.RootView, "Data"))
			require.Len(t, component.RootView.Columns, 2)
			amount := component.RootView.Columns[0]
			require.Equal(t, "Amount", amount.Name)
			require.Equal(t, "Amount", amount.Source)
			require.Equal(t, spec.TypeRef{Package: "example.com/anonymousscalar/amounts", Name: "Amount", Pointer: name == "PointerOutput"}, amount.Type)
			require.Equal(t, name == "PointerOutput", amount.Nullable)
			require.False(t, *amount.Groupable)
			require.Equal(t, "Region", component.RootView.Columns[1].Name)
			require.Equal(t, "region_code", component.RootView.Columns[1].Source)
			require.True(t, *component.RootView.Columns[1].Groupable)
		})
	}
	writeFile(t, root, "amounts/contract_test.go", anonymousScalarRuntimeTest)
	t.Run("linked-artifact-report", func(t *testing.T) {
		command := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "-timeout=90s", "-v", "./amounts")
		command.Dir = root
		command.Env = append(os.Environ(), "GOWORK=off")
		output, err := command.CombinedOutput()
		require.NoError(t, err, "compiled source consumer: %s", output)
		require.Contains(t, string(output), "--- PASS: TestAnonymousScalarArtifacts")
		t.Logf("compiled source ArtifactBuilder/report proof:\n%s", output)
	})
}

const anonymousScalarRuntimeTest = `package amounts
import (
 "reflect"
 "testing"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/report"
 "github.com/viant/datly/spec"
)
func TestAnonymousScalarArtifacts(t *testing.T){
 for _,output:=range []reflect.Type{reflect.TypeOf(ValueOutput{}),reflect.TypeOf(PointerOutput{})}{
  t.Run(output.Name(),func(t *testing.T){
   component:=&spec.Component{
    Key:spec.Key{Kind:spec.KindComponent,Scope:"example.com/anonymousscalar/amounts",Name:"Amounts"},
    Routes:[]*spec.Route{{Method:"GET",Path:"/amounts"}},
    Settings:&spec.Settings{Report:&spec.ReportSettings{Enabled:true,Compose:&spec.CubeComposeSettings{Enabled:true}}},
   }
   input:=bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(struct{}{}),OutputType:output}
   artifact,err:=bootstrap.BuildArtifact(input)
   if err!=nil{t.Fatal(err)}
   cols:=artifact.Component.RootView.Columns
   if len(cols)!=2||cols[0].Name!="Amount"||cols[0].Source!="Amount"||cols[1].Name!="Region"||cols[1].Source!="region_code"{t.Fatalf("canonical columns=%+v",cols)}
   if cols[0].Type.Name!="Amount"||cols[0].Type.Package!="example.com/anonymousscalar/amounts"||cols[0].Nullable!=(output.Name()=="PointerOutput")||cols[0].Type.Pointer!=cols[0].Nullable{t.Fatalf("named scalar=%+v",cols[0])}
   if !reflect.DeepEqual(cols,artifact.Reader.Root.View.Spec.Columns){t.Fatal("reader canonical spec diverged")}
   if component.RootView!=nil{t.Fatal("artifact mutated declaration")}
   compilation,err:=report.NewProjectCompiler(report.ProjectConfig{}).CompileArtifacts([]bootstrap.ArtifactInput{input})
   if err!=nil{t.Fatal(err)}
   artifacts:=compilation.Artifacts()
   if len(artifacts)!=3{t.Fatalf("source/cube/compose artifacts=%d",len(artifacts))}
   routes:=map[string]bool{}
   for _,a:=range artifacts{routes[a.Component().Routes[0].Path]=true}
   if !routes["/amounts"]||!routes["/amounts/cube"]||!routes["/amounts/cube/compose"]{t.Fatalf("derived routes=%v",routes)}
   // Explicit emptiness remains authoritative even for a valid embedded scalar.
   declaration:=artifact.Component.Clone()
   declaration.RootView.Columns=[]*spec.Column{}
   input.Component=declaration
   empty,err:=bootstrap.BuildArtifact(input)
   if err!=nil{t.Fatal(err)}
   if empty.Component.RootView.Columns==nil||len(empty.Component.RootView.Columns)!=0{t.Fatal("explicit empty projection replaced")}
  })
 }
}
`
