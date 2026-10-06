package transcribe

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/dql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

const factoryInputProjection = `SELECT a.TARGET AS Target,a.EXCLUSION AS Exclusion,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),tag(a.TARGET,'sqlx:"-"'),tag(a.EXCLUSION,'sqlx:"-"') FROM (CI_AUDIENCE) a`

func TestFactoryInputShapeStructuralBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, SQL string
		valid     bool
	}{
		{"single auxiliary leaf", factoryInputProjection, true},
		{"operational root", "SELECT a.TARGET AS Target FROM CI_AUDIENCE a", false},
		{"extra statement", factoryInputProjection + "; DELETE FROM CI_AUDIENCE", false},
		{"join", "SELECT a.TARGET AS Target FROM (CI_AUDIENCE) a JOIN (OTHER) b ON b.ID=a.ID", false},
		{"computed expression", "SELECT CONCAT(a.TARGET,'x') AS Target FROM (CI_AUDIENCE) a", false},
		{"wildcard", "SELECT a.* FROM (CI_AUDIENCE) a", false},
		{"filter", "SELECT a.TARGET AS Target FROM (CI_AUDIENCE) a WHERE a.TARGET IS NOT NULL", false},
		{"union", "SELECT a.TARGET AS Target FROM (CI_AUDIENCE) a UNION SELECT b.TARGET AS Target FROM (CI_AUDIENCE) b", false},
		{"writable metadata", "SELECT a.TARGET AS Target,writer_action_policy(a,'insert-delete') FROM (CI_AUDIENCE) a", false},
		{"view connector", "SELECT a.TARGET AS Target,use_connector(a,'mysql') FROM (CI_AUDIENCE) a", false},
		{"service", "$sql.Insert($Data, 'CI_AUDIENCE')", false},
		{"template", "#if($Data) " + factoryInputProjection + " #end", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := dql.PrepareSource(tc.SQL)
			err := p.Err()
			if err == nil {
				err = validateFactoryProjection(p)
			}
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				t.Log(err)
			}
		})
	}
}

func factoryInputShapeFixture(t *testing.T) (string, *Source) {
	t.Helper()
	root, source := generatedPostFactoryFixture(t)
	writeSourceHandlerFile(t, root, "archive/business.go", `package archive
import("context";"github.com/viant/xdatly/handler")
type Data struct { Data []*Res }
type OutputRes struct { TargetStr string; ExclusionStr string; Trusted map[string]int `+"`json:\"-\"`"+` }
var observed *ArchiveInput
var observedOutput *ArchiveOutput
var calls int
type Handler struct{}
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(ctx context.Context,_ handler.Session,in *ArchiveInput,out *ArchiveOutput)error{if err:=ctx.Err();err!=nil{return err};observed=in;observedOutput=out;calls++;out.Status="ok";for _,r:=range in.InpData{value:=&OutputRes{};if r!=nil{value.TargetStr=r.Target;value.ExclusionStr=r.Exclusion};out.Data=append(out.Data,value)};return nil}
`)
	source.Text = fmt.Sprintf(`#package(%q)
#import('archive',%q)
#setting($_ = $handler_factory('archive.NewArchive','Archive'))
#setting($_ = $route('/archive','PATCH'))
#setting($_ = $internal(true))
#setting($_ = $connector('shape-discovery-only'))
#setting($_ = $input_type('ArchiveInput'))
#setting($_ = $output_type('ArchiveOutput'))
#define($_ = $InpData<[]*Res>(body/data).Optional())
#define($_ = $Status<string>(output/status))
#define($_ = $Data<[]*archive.OutputRes>(output/body).WithTag('json:"data"'))
`, handlerFixtureModule+"/archive", handlerFixtureModule+"/archive") + factoryInputProjection
	source.Connector = "shape-discovery-only"
	return root, source
}
func TestFactoryInputShapeNativeGeneration(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "shape.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT, EXCLUSION TEXT)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	writeSourceHandlerFile(t, root, "dql/shape.dql", source.Text)
	before := sourceHandlerSnapshot(t, root)
	var first map[string]string
	for round := 0; round < 2; round++ {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Views)
		require.Empty(t, compiled.Component.Settings.DefaultConnector)
		require.Empty(t, compiled.Component.Settings.Mutation)
		direct, err := NewCompiler().Compile(context.Background(), compiled.Source)
		require.NoError(t, err)
		require.Equal(t, compiled.Component, direct.Component)
		require.Equal(t, compiled.ExternalHandler.InputShape, direct.ExternalHandler.InputShape)
		project, err := (&Discovery{BaseDir: root, Include: []string{handlerFixtureModule + "/dql"}, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).Compile(context.Background())
		require.NoError(t, err)
		require.Len(t, project.Components, 1)
		require.Equal(t, compiled.Component, project.Components[0].Component)
		require.Equal(t, compiled.ExternalHandler.InputShape, project.Components[0].ExternalHandler.InputShape)
		shape := compiled.ExternalHandler.InputShape
		require.NotNil(t, shape)
		require.Equal(t, "Res", shape.TypeName)
		require.Equal(t, "CI_AUDIENCE", shape.Source.Table)
		require.True(t, shape.Auxiliary)
		require.Len(t, shape.Columns, 2)
		for _, c := range shape.Columns {
			require.Equal(t, "TEXT", strings.ToUpper(c.DatabaseType))
			require.Equal(t, "string", c.EffectiveType().Name)
			require.False(t, c.EffectiveType().Pointer)
			require.Equal(t, `sqlx:"-"`, c.Tag)
		}
		generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		plan := generated.Result.Plan
		require.Nil(t, plan.MutationHandler)
		require.Empty(t, plan.Connector)
		require.Empty(t, plan.RootSource)
		require.Empty(t, plan.RootViewName)
		require.Len(t, plan.Views, 1)
		require.Equal(t, "Res", plan.Views[0].Type)
		now := sourceHandlerSnapshot(t, root)
		require.Equal(t, before[filepath.Join(root, "archive/business.go")], now[filepath.Join(root, "archive/business.go")])
		require.FileExists(t, filepath.Join(root, "archive/res.go"))
		router, err := os.ReadFile(filepath.Join(root, "archive/router.go"))
		require.NoError(t, err)
		require.NotContains(t, string(router), "shape-discovery-only")
		require.NotContains(t, string(router), "CI_AUDIENCE")
		if first == nil {
			first = now
		} else {
			require.Equal(t, first, now)
		}
	}
	writeSourceHandlerFile(t, root, "archive/runtime_test.go", factoryInputShapeRuntime)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-timeout=2m", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	t.Logf("native source-index runtime:\n%s", output)
}

func TestFactoryInputShapeFailuresDoNotPublish(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement, authored string }{
		{name: "operational root", old: "FROM (CI_AUDIENCE) a", replacement: "FROM CI_AUDIENCE a"},
		{name: "additional statement", old: factoryInputProjection, replacement: factoryInputProjection + "; DELETE FROM CI_AUDIENCE"},
		{name: "current binding", old: "#define($_ = $InpData", replacement: "#define($_ = $Current<[]*Res>(view/Current))\n#define($_ = $InpData"},
		{name: "imported body", old: "$InpData<[]*Res>", replacement: "$InpData<[]*archive.Res>"},
		{name: "alternate output type", old: "$InpData<[]*Res>", replacement: "$InpData<[]*Res,[]*archive.OutputRes>"},
		{name: "same output type", old: "$InpData<[]*Res>", replacement: "$InpData<[]*Res,[]*Res>"},
		{name: "body codec", old: "$InpData<[]*Res>(body/data)", replacement: "$InpData<[]*Res>(body/data).WithCodec('JSON')"},
		{name: "body output emission", old: "$InpData<[]*Res>(body/data)", replacement: "$InpData<[]*Res>(body/data).Output()"},
		{name: "queue metadata", old: "type(a,'Res')", replacement: "type(a,'Res'),queue_contract(a,'source-slice')"},
		{name: "competing authored Res", authored: "type Res struct { Target string; Exclusion string }"},
		{name: "competing local alias", authored: "type Res = OutputRes"},
		{name: "runtime SQL tag", old: `sqlx:"-"`, replacement: `sqlx:"-" sql:"SELECT 1"`},
		{name: "mutation setting", old: factoryInputProjection, replacement: "#setting($_ = $mutation('patch'))\n" + factoryInputProjection},
		{name: "service program", old: factoryInputProjection, replacement: "$sql.Insert($Data,'CI_AUDIENCE')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			if tc.old != "" {
				require.Contains(t, source.Text, tc.old)
				source.Text = strings.Replace(source.Text, tc.old, tc.replacement, 1)
			}
			if tc.authored != "" {
				writeSourceHandlerFile(t, root, "archive/conflict.go", "package archive\n"+tc.authored+"\n")
			}
			before := sourceHandlerSnapshot(t, root)
			db := &forbiddenHandlerDB{}
			source.ColumnRefiner = column.New(db)
			_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
			require.Error(t, err)
			require.Zero(t, db.calls)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
			t.Log(err)
		})
	}
}

const factoryInputShapeRuntime = `package archive
import("context";"errors";"os";"path/filepath";"reflect";"strings";"testing";"net/http/httptest";
 "github.com/stretchr/testify/require";"github.com/viant/datly/bootstrap";"github.com/viant/datly/spec";"github.com/viant/datly/standalone";"github.com/viant/datly/standalone/config";dexec "github.com/viant/datly/exec";dtag "github.com/viant/datly/tag";requestprovider "github.com/viant/bindly/provider/request")
func TestFactoryShapeRuntime(t *testing.T){
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok);metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 require.Nil(t,component.RootView);require.Empty(t,component.Views);require.Empty(t,component.Settings.DefaultConnector);require.Empty(t,component.Settings.Mutation)
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[ArchiveInput](),OutputType:reflect.TypeFor[ArchiveOutput]()});require.NoError(t,err);require.Empty(t,artifact.ViewDependencies)
 require.Equal(t,reflect.TypeFor[[]*Res](),reflect.TypeOf(Data{}.Data));typ:=reflect.TypeFor[Res]();require.Equal(t,2,typ.NumField());for _,name:=range []string{"Target","Exclusion"}{f,ok:=typ.FieldByName(name);require.True(t,ok);require.Equal(t,reflect.TypeFor[string](),f.Type);require.Equal(t,"-",f.Tag.Get("sqlx"))}
 cwd,err:=os.Getwd();require.NoError(t,err)
 for _,eager:=range []bool{false,true}{ctx,cancel:=context.WithCancel(context.Background());server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:filepath.Dir(cwd),GoBootstrap:&config.Packages{Packages:[]string{"github.com/viant/datly/handlerfixture/archive"},EagerComponents:eager},Endpoint:config.Endpoint{Address:"127.0.0.1:0"}},Holders:[]any{ArchiveDatly}});require.NoError(t,err);require.NoError(t,server.Reload(ctx,1))
 target:=dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"github.com/viant/datly/handlerfixture/archive",Name:"Archive"},Route:spec.RouteRef{Method:"PATCH",Path:"/archive"}}
 for _,tc:=range []struct{name,body string;nilInput,present bool;count int}{{"absent","{}",true,false,0},{"null","{\"data\":null}",true,true,0},{"empty","{\"data\":[]}",false,true,0},{"multi","{\"data\":[{\"target\":\"location:[]\"},null,{\"exclusion\":\"device:[]\"}]}",false,true,3}}{t.Run(tc.name,func(t *testing.T){req:=httptest.NewRequest("PATCH","/archive",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json");public:=httptest.NewRecorder();server.ServeHTTP(public,req);require.Equal(t,404,public.Code)
 req=httptest.NewRequest("PATCH","/archive",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(req);require.NoError(t,err);value,err:=server.InvokeComponent(ctx,dexec.ComponentRequest{Target:target,Providers:scope.Providers()});require.NoError(t,err);require.Same(t,observedOutput,value.(*ArchiveOutput));require.Equal(t,tc.nilInput,observed.InpData==nil);if tc.present{require.NotNil(t,observed.Has);require.True(t,observed.Has.InpData)}else{require.Nil(t,observed.Has)};require.Len(t,value.(*ArchiveOutput).Data,tc.count);if tc.count==3{require.Nil(t,observed.InpData[1]);require.Equal(t,"location:[]",value.(*ArchiveOutput).Data[0].TargetStr);require.Empty(t,value.(*ArchiveOutput).Data[1].TargetStr);require.Equal(t,"device:[]",value.(*ArchiveOutput).Data[2].ExclusionStr)}})}
 typed:=&ArchiveInput{InpData:[]*Res{{Target:"typed"}},Has:&ArchiveInputHas{InpData:true}};value,err:=server.InvokeComponent(ctx,dexec.ComponentRequest{Target:target,Input:typed});require.NoError(t,err);require.Same(t,typed,observed);require.Same(t,observedOutput,value.(*ArchiveOutput))
 stopped,stop:=context.WithCancel(ctx);stop();before:=calls;_,err=server.InvokeComponent(stopped,dexec.ComponentRequest{Target:target,Input:typed});require.ErrorIs(t,err,context.Canceled);require.True(t,errors.Is(err,context.Canceled));require.Equal(t,before,calls)
 cancel();require.NoError(t,server.Shutdown(context.Background()))
 }
}
`

func TestFactoryInputShapeExpandedBoundary(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"expanded operational root", strings.Replace(factoryInputProjection, "(CI_AUDIENCE)", "CI_AUDIENCE", 1)},
		{"expanded additional statement", factoryInputProjection + "; DELETE FROM CI_AUDIENCE"},
		{"expanded service", `$sql.Insert($Data,'CI_AUDIENCE')`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			source.Text = strings.Replace(source.Text, factoryInputProjection, "${embed:projection.sql}", 1)
			store := resource.New()
			require.NoError(t, store.Register("", fstest.MapFS{"projection.sql": &fstest.MapFile{Data: []byte(tc.sql)}}))
			source.Resources = store
			db := &forbiddenHandlerDB{}
			source.ColumnRefiner = column.New(db)
			before := sourceHandlerSnapshot(t, root)
			_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
			require.Error(t, err)
			require.Zero(t, db.calls)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
			t.Log(err)
		})
	}
}

func TestFactoryInputShapeStagedSignatureFailureAtomic(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	file := filepath.Join(root, "archive/business.go")
	business, err := os.ReadFile(file)
	require.NoError(t, err)
	business = []byte(strings.Replace(string(business), "func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }", "func NewArchive() *Handler { return &Handler{} }", 1))
	require.NotContains(t, string(business), "func NewArchive() handler.Contract")
	require.NoError(t, os.WriteFile(file, business, 0600))
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "shape.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	before := sourceHandlerSnapshot(t, root)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
	require.NoError(t, err)
	_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	require.ErrorContains(t, err, "handler source build")
	require.Equal(t, before, sourceHandlerSnapshot(t, root))
	t.Log(err)
}
