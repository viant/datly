package transcribe

import (
	"database/sql"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
)

func TestFactoryTypedExternalCurrentNativeGeneration(t *testing.T) {
	for _, qualifier := range []string{"shared", handlerFixtureModule + "/shared"} {
		t.Run(qualifier, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			foreign := `package shared
import "reflect"
type Record struct { Id int ` + "`sqlx:\"ID\"`" + `; Value string ` + "`sqlx:\"VALUE\"`" + `; Has *RecordHas ` + "`setMarker:\"true\" sqlx:\"-\" json:\"-\"`" + ` }
type RecordHas struct { Id, Value bool }
var _anchorRecord = reflect.TypeFor[Record]()
func init() { panic("source discovery must not execute init") }
`
			runtimeFixture := qualifier != "shared"
			if runtimeFixture {
				foreign = strings.Replace(foreign, `func init() { panic("source discovery must not execute init") }`, "", 1)
			}
			writeSourceHandlerFile(t, root, "shared/record.go", foreign)
			source.Text = strings.Replace(source.Text, "#setting", fmt.Sprintf("#import('shared',%q)\n#setting", handlerFixtureModule+"/shared"), 1)
			source.Text = strings.Replace(source.Text, "#define($_ = $Status", fmt.Sprintf("#define($_ = $Current<[]*%s.Record>(view/Current).Cardinality('Many') /* SELECT * FROM RECORDS */)\n#define($_ = $Status", qualifier), 1)
			source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),tag(a.TARGET,'sqlx:"-"'),tag(a.EXCLUSION,'sqlx:"-"'),c.*,type(c,'`+qualifier+`.Record','Children'),required(c.ID),required(c.VALUE) FROM CI_AUDIENCE a LEFT JOIN RECORDS c ON a.TARGET=c.VALUE`, 1)
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT); CREATE TABLE RECORDS(ID INTEGER NOT NULL,VALUE TEXT NOT NULL)")
			require.NoError(t, err)
			source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
			var stable map[string]string
			for round := 0; round < 3; round++ {
				if round == 2 {
					_, err = db.Exec("ALTER TABLE CI_AUDIENCE ADD COLUMN PUBLIC_NOTE TEXT")
					require.NoError(t, err)
				}
				compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
				require.NoError(t, err)
				generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
				require.NoError(t, err)
				linked := 0
				for _, view := range generated.Result.Plan.Views {
					if view.Package == handlerFixtureModule+"/shared" {
						require.Equal(t, gen.ViewLinked, view.Ownership)
						require.Empty(t, view.Destination)
						linked++
					}
				}
				require.Equal(t, 2, linked)
				snapshot := sourceHandlerSnapshot(t, root)
				require.Equal(t, foreign, snapshot[filepath.Join(root, "shared/record.go")])
				if round == 0 {
					stable = snapshot
				} else if round == 1 {
					require.Equal(t, stable, snapshot)
				} else {
					require.Contains(t, snapshot[filepath.Join(root, "archive/res.go")], "PublicNote")
				}
			}
			if runtimeFixture {
				writeSourceHandlerFile(t, root, "archive/current_runtime_test.go", factoryTypedCurrentRuntime)
				cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=2m", "./archive")
				cmd.Dir = root
				output, err := cmd.CombinedOutput()
				require.NoError(t, err, "%s", output)
				t.Logf("native typed Current and external body runtime: %s", output)
			}
		})
	}
}

func TestFactoryTypedExternalCurrentRejectsConflictingAuthority(t *testing.T) {
	for _, tc := range []struct{ name, row, expression, options, want string }{
		{name: "explicit view mismatch", options: ".TypeName('archive.Res')", want: "does not match package type"},
		{name: "inverse view mismatch", expression: "[]*archive.Res", options: ".TypeName('shared.Record')", want: "does not match package type"},
		{name: "output mismatch", expression: "[]*shared.Record,[]*archive.Res", want: "conflicting type"},
		{name: "output wrapper mismatch", expression: "[]*shared.Record,*shared.Record", want: "conflicting type wrappers"},
		{name: "tag mismatch", options: ".WithTag('typeName:\"archive.Res\"')", want: "conflicting type"},
		{name: "destination", options: ".Dest('record.go')", want: "destination"},
		{name: "one cardinality", options: ".Cardinality('One')", want: "cardinality"},
		{name: "named slice", row: "type Record []*Element; type Element struct { Id int `sqlx:\"ID\"` }", want: "must be a named struct"},
		{name: "named pointer", row: "type Record *Element; type Element struct { Id int `sqlx:\"ID\"` }", want: "must be a named struct"},
		{name: "missing", expression: "[]*shared.Missing", want: "exact named type"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			row := tc.row
			if row == "" {
				row = "type Record struct { Id int `sqlx:\"ID\"` }"
			}
			writeSourceHandlerFile(t, root, "shared/record.go", "package shared\n"+row+"\n")
			source.Text = strings.Replace(source.Text, "#setting", fmt.Sprintf("#import('shared',%q)\n#setting", handlerFixtureModule+"/shared"), 1)
			expression := tc.expression
			if expression == "" {
				expression = "[]*shared.Record"
			}
			options := tc.options
			if !strings.Contains(options, "Cardinality") {
				options = ".Cardinality('Many')" + options
			}
			source.Text = strings.Replace(source.Text, "#define($_ = $Status", fmt.Sprintf("#define($_ = $Current<%s>(view/Current)%s /* SELECT * FROM RECORDS */)\n#define($_ = $Status", expression, options), 1)
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "schema.db"))
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT); CREATE TABLE RECORDS(ID INTEGER NOT NULL)")
			require.NoError(t, err)
			source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
			before := sourceHandlerSnapshot(t, root)
			_, err = (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

const factoryTypedCurrentRuntime = `package archive
import (
 "context"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 "github.com/stretchr/testify/require"
 "github.com/viant/bindly/locator"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/handlerfixture/shared"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 rhandler "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
)
func TestTypedCurrentReadAndSharedBodyIdentity(t *testing.T) {
 ctx:=context.Background();db:=sqlite.New(t)
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE RECORDS(ID INTEGER NOT NULL,VALUE TEXT NOT NULL)","INSERT INTO RECORDS(ID,VALUE) VALUES(7,'loaded')"))
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok)
 metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 require.Nil(t,component.RootView);require.Empty(t,component.Settings.Mutation);require.Len(t,component.Views,1)
 resources:=resource.New();require.NoError(t,resources.Register(ArchiveComponent{}.EmbedNamespace(),ArchiveComponent{}.EmbedFS()))
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[ArchiveInput](),OutputType:reflect.TypeFor[ArchiveOutput](),Resources:resources});require.NoError(t,err)
 require.Len(t,artifact.ViewDependencies,1)
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});require.NoError(t,err)
 checked:=false
 business:=rhandler.HandlerFunc(func(ctx context.Context,invocation rhandler.Invocation)(any,error){
  in:=invocation.Input.(*ArchiveInput)
  require.Len(t,in.Current,1);require.IsType(t,&shared.Record{},in.Current[0]);require.Equal(t,7,in.Current[0].Id);require.Equal(t,"loaded",in.Current[0].Value);require.Nil(t,in.Current[0].Has,"database reads must not fabricate PATCH presence")
  require.Len(t,in.InpData,1);require.Len(t,in.InpData[0].Children,1)
  child:=in.InpData[0].Children[0];require.NotNil(t,child.Has);require.True(t,child.Has.Id);require.False(t,child.Has.Value);require.Zero(t,child.Id)
  in.InpData[0].Children[0]=in.Current[0];require.Same(t,in.Current[0],in.InpData[0].Children[0],"business composition must reuse the exact loaded pointer")
  checked=true;return &ArchiveOutput{Status:"ok"},nil
 })
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[ArchiveOutput](),Handler:business,Providers:[]locator.Provider{views}}},druntime.WithResources(resources));require.NoError(t,err)
 req:=httptest.NewRequest("PATCH","/archive",strings.NewReader("{\"data\":[{\"children\":[{\"id\":0}]}]}"));req.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(req);require.NoError(t,err);defer scope.Close()
 _,err=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:holder.PkgPath(),Name:"Archive"},Route:spec.RouteRef{Method:"PATCH",Path:"/archive"}},Providers:scope.Providers()});require.NoError(t,err);require.True(t,checked)
 var count int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM RECORDS").Scan(&count));require.Equal(t,1,count)
}
`
