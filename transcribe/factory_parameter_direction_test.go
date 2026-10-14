package transcribe

import (
	"github.com/stretchr/testify/require"
	"os/exec"
	"strings"
	"testing"
)

func TestFactorySameNamedInputOutputCurrentDependency(t *testing.T) {
	for _, outputFirst := range []bool{false, true} {
		name := "input-first"
		if outputFirst {
			name = "output-first"
		}
		t.Run(name, func(t *testing.T) {
			root, source, db := localCurrentBodyFixture(t, "[]*archive.Record", "")
			_, err := db.Exec("INSERT INTO RECORDS(ID,VALUE) VALUES(7,'loaded'),(8,'other')")
			require.NoError(t, err)
			writeSourceHandlerFile(t, root, "archive/business.go", `package archive
import("context";"github.com/viant/xdatly/handler")
type OutputRes struct { Id int `+"`json:\"id\"`"+` }
type Handler struct{}
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(_ context.Context,_ handler.Session,in *ArchiveInput,out *ArchiveOutput)error{out.Status="ok";for _,r:=range in.Current{out.Data=append(out.Data,&OutputRes{Id:r.Id})};return nil}
`)
			source.Text = strings.Replace(source.Text, "$InpData<[]*Res>", "$Data<*Res>", 1)
			source.Text = strings.Replace(source.Text, "SELECT ID,VALUE FROM RECORDS", "SELECT ID,VALUE FROM RECORDS WHERE $criteria.In(\"ID\", $Data.Ids)", 1)
			source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT request.*,type(request,'Res'),cast(request.IDS AS []int),required(request.IDS) FROM (SELECT JSON_GROUP_ARRAY(ID) AS IDS FROM RECORDS) request`, 1)
			inputDecl := "#define($_ = $Data<*Res>(body/data).Optional())"
			outputDecl := `#define($_ = $Data<[]*archive.OutputRes>(output/body).WithTag('json:"data"'))`
			if outputFirst {
				source.Text = strings.Replace(source.Text, inputDecl, "", 1)
				source.Text = strings.Replace(source.Text, outputDecl, outputDecl+"\n"+inputDecl, 1)
			}
			source.Text = strings.Replace(source.Text, "#setting", "#setting($_ = $case_format('lc'))\n#setting", 1)
			before := source.Text
			var stable map[string]string
			for round := 0; round < 2; round++ {
				compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
				require.NoError(t, err)
				generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
				require.NoError(t, err)
				require.Equal(t, before, source.Text)
				plan := generated.Result.Plan
				found := false
				for _, field := range plan.Input.Fields {
					if field.Name == "Data" {
						require.Equal(t, "*Res", field.Type)
						found = true
					}
				}
				require.True(t, found)
				snapshot := sourceHandlerSnapshot(t, root)
				if round == 0 {
					stable = snapshot
				} else {
					require.Equal(t, stable, snapshot)
				}
			}
			writeSourceHandlerFile(t, root, "archive/runtime_test.go", factoryDirectionRuntime)
			cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-timeout=2m", "./archive")
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
			require.Contains(t, string(output), "--- PASS: TestSameNamedDataRuntime")
			t.Logf("%s", output)
		})
	}
}

const factoryDirectionRuntime = `package archive
import (
 "context";"encoding/json";"net/http/httptest";"reflect";"strings";"testing"
 "github.com/stretchr/testify/require"
 "github.com/viant/bindly/locator";"github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap";dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime";rhandler "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql"
 viewprovider "github.com/viant/datly/sql/reader/provider";dtag "github.com/viant/datly/tag"
)
func TestSameNamedDataRuntime(t *testing.T) {
 ctx:=context.Background();db:=sqlite.New(t)
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE RECORDS(ID INTEGER NOT NULL,VALUE TEXT NOT NULL)","INSERT INTO RECORDS VALUES(7,'loaded'),(8,'other')"))
 f,ok:=reflect.TypeFor[ArchiveInput]().FieldByName("Data");require.True(t,ok);require.Equal(t,reflect.TypeFor[*Res](),f.Type)
 f,ok=reflect.TypeFor[ArchiveOutput]().FieldByName("Data");require.True(t,ok);require.Equal(t,reflect.TypeFor[[]*OutputRes](),f.Type)
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok)
 metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 resources:=resource.New();require.NoError(t,resources.Register(ArchiveComponent{}.EmbedNamespace(),ArchiveComponent{}.EmbedFS()))
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[ArchiveInput](),OutputType:reflect.TypeFor[ArchiveOutput](),Resources:resources});require.NoError(t,err)
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});require.NoError(t,err)
 business:=rhandler.HandlerFunc(func(ctx context.Context,invocation rhandler.Invocation)(any,error){
  in:=invocation.Input.(*ArchiveInput);require.NotNil(t,in.Data);require.Equal(t,[]int{7},in.Data.Ids);require.Len(t,in.Current,1);require.Equal(t,7,in.Current[0].Id)
  out:=&ArchiveOutput{};require.NoError(t,NewArchive().Exec(ctx,nil,in,out));return out,nil
 })
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[ArchiveOutput](),Handler:business,Providers:[]locator.Provider{views}}},druntime.WithResources(resources));require.NoError(t,err)
 req:=httptest.NewRequest("PATCH","/archive",strings.NewReader("{\"data\":{\"ids\":[7]}}"));req.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(req);require.NoError(t,err);defer scope.Close()
 value,err:=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:holder.PkgPath(),Name:"Archive"},Route:spec.RouteRef{Method:"PATCH",Path:"/archive"}},Providers:scope.Providers()});require.NoError(t,err)
 encoded,err:=json.Marshal(value);require.NoError(t,err);require.JSONEq(t,"{\"status\":\"ok\",\"data\":[{\"id\":7}]}",string(encoded))
 var count int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM RECORDS").Scan(&count));require.Equal(t,2,count)
}
`
