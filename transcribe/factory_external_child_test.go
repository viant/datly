package transcribe

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
)

func TestFactoryInputShapeExternalChildNativeGeneration(t *testing.T) {
	for _, qualifier := range []string{"shared", handlerFixtureModule + "/shared"} {
		t.Run(qualifier, func(t *testing.T) { testFactoryExternalChildNativeGeneration(t, qualifier) })
	}
}

func TestFactoryExternalChildRejectsNamedRowContainersWithoutPublication(t *testing.T) {
	for _, expression := range []string{"[]Element", "[1]Element", "*Element"} {
		t.Run(expression, func(t *testing.T) {
			root, source := factoryInputShapeFixture(t)
			writeSourceHandlerFile(t, root, "shared/child.go", "package shared\ntype Child "+expression+"\ntype Element struct { Id int `sqlx:\"ID\"`; ParentTarget string `sqlx:\"PARENT_TARGET\"`; Has *Has `setMarker:\"true\" json:\"-\"` }\ntype Has struct { Id, ParentTarget bool }\n")
			source.Text = strings.Replace(source.Text, "#setting", fmt.Sprintf("#import('shared',%q)\n#setting", handlerFixtureModule+"/shared"), 1)
			source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),c.*,type(c,'shared.Child','Children') FROM CI_AUDIENCE a LEFT JOIN CHILDREN c ON a.TARGET=c.PARENT_TARGET`, 1)
			db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "shape.db"))
			require.NoError(t, err)
			defer db.Close()
			_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT); CREATE TABLE CHILDREN(ID INTEGER NOT NULL,PARENT_TARGET TEXT NOT NULL)")
			require.NoError(t, err)
			source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
			before := sourceHandlerSnapshot(t, root)
			_, err = (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
			require.ErrorContains(t, err, "must be a named struct")
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
		})
	}
}

func testFactoryExternalChildNativeGeneration(t *testing.T, qualifier string) {
	root, source := factoryInputShapeFixture(t)
	rowSource := `package shared
type Child struct {
 Id int ` + "`sqlx:\"ID\" json:\"id\"`" + `
 ParentTarget string ` + "`sqlx:\"PARENT_TARGET\" json:\"parentTarget\"`" + `
 Has *ChildHas ` + "`setMarker:\"true\" sqlx:\"-\" json:\"-\"`" + `
}
type ChildHas struct { Id, ParentTarget bool }
func init() { panic("external child source discovery must not execute initialization") }
`
	runtimeFixture := qualifier != "shared"
	if runtimeFixture {
		rowSource = strings.Replace(rowSource, `func init() { panic("external child source discovery must not execute initialization") }`, "", 1)
		source.Text = strings.Replace(source.Text, "#setting", "#setting($_ = $component_call_policy('buffered'))\n#setting", 1)
	}
	writeSourceHandlerFile(t, root, "shared/child.go", rowSource)
	source.Text = strings.Replace(source.Text, "#setting", fmt.Sprintf("#import('shared',%q)\n#setting", handlerFixtureModule+"/shared"), 1)
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),tag(a.TARGET,'sqlx:"-"'),tag(a.EXCLUSION,'sqlx:"-"'),
 c.*,type(c,'shared.Child','Children'),required(c.ID),required(c.PARENT_TARGET)
 FROM (CI_AUDIENCE) a LEFT JOIN (CHILDREN) c ON a.TARGET = c.PARENT_TARGET`, 1)
	source.Text = strings.Replace(source.Text, "shared.Child", qualifier+".Child", 1)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "shape.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE CI_AUDIENCE(TARGET TEXT,EXCLUSION TEXT); CREATE TABLE CHILDREN(ID INTEGER NOT NULL,PARENT_TARGET TEXT NOT NULL)")
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	writeSourceHandlerFile(t, root, "dql/shape.dql", source.Text)
	var stable map[string]string
	for round := 0; round < 3; round++ {
		if round == 2 {
			_, err = db.Exec("ALTER TABLE CI_AUDIENCE ADD COLUMN PUBLIC_NOTE TEXT")
			require.NoError(t, err)
		}
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.NotNil(t, compiled.ExternalHandler.InputShape)
		generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		linked := 0
		for _, view := range generated.Result.Plan.Views {
			if view.Package == handlerFixtureModule+"/shared" {
				require.Equal(t, gen.ViewLinked, view.Ownership)
				require.True(t, strings.HasSuffix(view.Type, ".Child"), view.Type)
				require.Empty(t, view.Destination)
				linked++
			}
		}
		require.Equal(t, 1, linked)
		body, err := os.ReadFile(filepath.Join(root, "archive/res.go"))
		require.NoError(t, err)
		require.Contains(t, string(body), "[]*shared.Child")
		if round == 2 {
			require.Contains(t, string(body), "PublicNote", "schema evolution must update the local wildcard body")
		}
		current := sourceHandlerSnapshot(t, root)
		if round == 0 {
			stable = current
		} else if round == 1 {
			require.Equal(t, stable, current, "unchanged native regeneration must be stable")
		}
		require.NotContains(t, string(body), "type Child struct")
		require.NotContains(t, string(body), "type ChildHas struct")
		foreign, err := os.ReadFile(filepath.Join(root, "shared/child.go"))
		require.NoError(t, err)
		require.Equal(t, rowSource, string(foreign))
	}
	if runtimeFixture {
		runtime := strings.Replace(factoryInputShapeRuntime, "import(", "import(\""+handlerFixtureModule+"/shared\";", 1)
		runtime = strings.Replace(runtime, "require.Equal(t,3,typ.NumField())", "require.Equal(t,5,typ.NumField());childField,ok:=typ.FieldByName(\"Children\");require.True(t,ok);require.Equal(t,reflect.TypeFor[[]*shared.Child](),childField.Type)", 1)
		runtime = strings.Replace(runtime, `\"target\":\"location:[]\"`, `\"target\":\"location:[]\",\"children\":[{\"id\":0}]`, 1)
		runtime = strings.Replace(runtime, "typed:=&ArchiveInput", "require.Len(t,observed.InpData[0].Children,1);boundChild:=observed.InpData[0].Children[0];require.NotNil(t,boundChild.Has);require.True(t,boundChild.Has.Id);require.False(t,boundChild.Has.ParentTarget);require.Zero(t,boundChild.Id)\n typed:=&ArchiveInput", 1)
		runtime = strings.Replace(runtime, `Target:"typed"`, `Target:"typed",Children:[]*shared.Child{boundChild}`, 1)
		runtime = strings.Replace(runtime, "require.Same(t,typed,observed)", "require.Same(t,typed,observed);require.Same(t,boundChild,observed.InpData[0].Children[0])", 1)
		writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtime)
		writeSourceHandlerFile(t, root, "archive/composition_test.go", factoryExternalCompositionRuntime)
		cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-timeout=2m", "./archive")
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Logf("external child binding and typed-pointer runtime: %s", output)
	}
}

const factoryExternalCompositionRuntime = `package archive
import (
 "context"
 "errors"
 "fmt"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 "github.com/stretchr/testify/require"
 "github.com/viant/datly/handlerfixture/shared"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/handler/custom"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/xdatly/handler"
)
type compositionAudit struct { Id int ` + "`sqlx:\"ID\"`" + `; Value string ` + "`sqlx:\"VALUE\"`" + ` }
type compositionInput struct { Rows []*shared.Child }
type compositionOutput struct { Rows []*shared.Child }
type compositionChild struct { mode string; dml handler.DML; observed *shared.Child }
func (h *compositionChild) Exec(ctx context.Context,s handler.Session,in *compositionInput,out *compositionOutput)error {
 v,ok,err:=s.Binder().Lookup(ctx,handler.DMLKey);if err!=nil{return err};if !ok{return fmt.Errorf("child DML missing")};h.dml=v.(handler.DML);h.observed=in.Rows[0]
 if err=h.dml.Insert("CHILDREN",in.Rows);err!=nil{return err}
 if h.mode=="child failure" { return errors.New("child rejected after enqueue") }
 if h.mode=="database failure" { if err=h.dml.Insert("CHILDREN",in.Rows);err!=nil{return err} }
 out.Rows=in.Rows;return nil
}
type compositionParent struct { t *testing.T; child *compositionChild; target dexec.ComponentTarget; count func()int; mode string }
func(h *compositionParent) Exec(ctx context.Context,s handler.Session,in *ArchiveInput,out *ArchiveOutput)error {
 v,ok,err:=s.Binder().Lookup(ctx,handler.DMLKey);if err!=nil{return err};if !ok{return fmt.Errorf("parent DML missing")};queue:=v.(handler.DML)
 if err=queue.Insert("AUDIT",[]*compositionAudit{{Id:1,Value:"before child"}});err!=nil{return err}
 v,ok,err=s.Binder().Lookup(ctx,dexec.ComponentInvokerKey);if err!=nil{return err};if !ok{return fmt.Errorf("scoped invoker missing")}
 _,err=v.(dexec.ComponentInvoker).InvokeComponent(ctx,dexec.ComponentRequest{Target:h.target,Input:&compositionInput{Rows:in.InpData[0].Children}})
 require.NotNil(h.t,h.child.dml,"child must receive invocation-scoped DML")
 require.Same(h.t,in.InpData[0].Children[0],h.child.observed,"external child pointer must be reused")
 require.Zero(h.t,h.count(),"child call must not flush either component's writes")
 if err!=nil{return err}
 if h.mode=="parent failure"{return errors.New("parent rejected after child")}
 if err=queue.Insert("AUDIT",[]*compositionAudit{{Id:2,Value:"after child"}});err!=nil{return err}
 out.Status="ok";return nil
}
func TestExternalBodySharedBufferAndRollback(t *testing.T) {
 for _,mode:=range []string{"success","child failure","parent failure","database failure"}{t.Run(mode,func(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t)
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE AUDIT(ID INTEGER PRIMARY KEY,VALUE TEXT)","CREATE TABLE CHILDREN(ID INTEGER PRIMARY KEY,PARENT_TARGET TEXT)","CREATE TABLE JOURNAL(SEQ INTEGER PRIMARY KEY AUTOINCREMENT,KIND TEXT)","CREATE TRIGGER audit_order AFTER INSERT ON AUDIT BEGIN INSERT INTO JOURNAL(KIND) VALUES(new.VALUE); END","CREATE TRIGGER child_order AFTER INSERT ON CHILDREN BEGIN INSERT INTO JOURNAL(KIND) VALUES('child'); END"))
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok);metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 parentSpec,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 require.Equal(t,"buffered",parentSpec.Settings.ComponentCallPolicy);require.Nil(t,parentSpec.RootView);require.Empty(t,parentSpec.Settings.Mutation)
 parentArtifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:parentSpec,InputType:reflect.TypeFor[ArchiveInput](),OutputType:reflect.TypeFor[ArchiveOutput]()});require.NoError(t,err)
 childSpec:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:holder.PkgPath(),Name:"Child"},Name:"Child",Routes:[]*spec.Route{{Method:"POST",Path:"/child"}}}
 childArtifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:childSpec,InputType:reflect.TypeFor[compositionInput](),OutputType:reflect.TypeFor[compositionOutput]()});require.NoError(t,err)
 child:=&compositionChild{mode:mode}
 count:=func()int{var n int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM JOURNAL").Scan(&n));return n}
 parent:=&compositionParent{t:t,child:child,mode:mode,count:count,target:dexec.ComponentTarget{Component:childSpec.Key,Route:spec.RouteRef{Method:"POST",Path:"/child"}}}
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{
 {Component:parentArtifact.Component,Input:parentArtifact.Input,Output:parentArtifact.Output,OutputType:reflect.TypeFor[ArchiveOutput](),Handler:custom.New(parent),DataSource:dml.Source{DB:db.DB}},
 {Component:childArtifact.Component,Input:childArtifact.Input,Output:childArtifact.Output,OutputType:reflect.TypeFor[compositionOutput](),Handler:custom.New(child),DataSource:dml.Source{DB:db.DB}},
 });require.NoError(t,err)
 req:=httptest.NewRequest("PATCH","/archive",strings.NewReader("{\"data\":[{\"children\":[{\"id\":7,\"parentTarget\":\"original pointer\"}]}]}"));req.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(req);require.NoError(t,err);defer scope.Close()
 _,err=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:parentSpec.Key,Route:spec.RouteRef{Method:"PATCH",Path:"/archive"}},Providers:scope.Providers()})
 if mode=="success"{require.NoError(t,err);require.Equal(t,3,count());rows,err:=db.DB.Query("SELECT KIND FROM JOURNAL ORDER BY SEQ");require.NoError(t,err);defer rows.Close();var order []string;for rows.Next(){var value string;require.NoError(t,rows.Scan(&value));order=append(order,value)};require.NoError(t,rows.Err());require.Equal(t,[]string{"before child","child","after child"},order)}else{require.Error(t,err);if mode=="child failure"{require.ErrorContains(t,err,"child rejected after enqueue")}else if mode=="parent failure"{require.ErrorContains(t,err,"parent rejected after child")}else{require.ErrorContains(t,err,"UNIQUE constraint")};require.Zero(t,count());for _,table:=range []string{"AUDIT","CHILDREN"}{var n int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&n));require.Zero(t,n)}}
 })}
}
`
