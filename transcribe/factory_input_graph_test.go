package transcribe

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/transcribe/column"
)

// A custom persistence owner uses a generated body graph and independently
// declared reads. The graph supplies types and presence, never a reader root.
func TestFactoryInputShapeJoinedCurrentGeneration(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	source.Text = strings.Replace(source.Text, "#setting($_ = $internal(true))\n", "", 1)
	driver, dsn := "sqlite3", filepath.Join(t.TempDir(), "graph.db")
	mysql := os.Getenv("FACTORY_GRAPH_MYSQL_DSN") != ""
	if mysql {
		driver, dsn = "mysql", os.Getenv("FACTORY_GRAPH_MYSQL_DSN")
	}
	db, err := sql.Open(driver, dsn)
	require.NoError(t, err)
	defer db.Close()
	schema := []string{`CREATE TABLE PARENT(ID INTEGER PRIMARY KEY AUTOINCREMENT,TARGET TEXT NOT NULL,EXCLUSION TEXT NOT NULL)`, `CREATE TABLE CHILD(ID INTEGER PRIMARY KEY AUTOINCREMENT,PARENT_ID INTEGER REFERENCES PARENT(ID),VALUE TEXT)`}
	if mysql {
		schema = []string{`CREATE TABLE PARENT(ID BIGINT PRIMARY KEY AUTO_INCREMENT,TARGET VARCHAR(255) NOT NULL,EXCLUSION VARCHAR(255) NOT NULL) ENGINE=InnoDB`, `CREATE TABLE CHILD(ID BIGINT PRIMARY KEY AUTO_INCREMENT,PARENT_ID BIGINT,VALUE VARCHAR(255),FOREIGN KEY(PARENT_ID) REFERENCES PARENT(ID)) ENGINE=InnoDB`}
	}
	for _, statement := range schema {
		_, err = db.Exec(statement)
		require.NoError(t, err)
	}
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*, c.*, type(a,'Res'), dest(a,'res.go'), type(c,'Child'), dest(c,'child.go'), required(a.TARGET), required(a.EXCLUSION) FROM (SELECT * FROM PARENT) a JOIN (SELECT * FROM CHILD) c ON c.PARENT_ID=a.ID`, 1)
	source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $RecordIDs<?>(param/InpData) /* ? SELECT ARRAY_AGG(Id) AS IDs FROM `+"`/`"+` LIMIT 1 */)
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CurrentRow').Dest('current.go') /* SELECT p.*,CAST(p.TARGET AS string) FROM PARENT p WHERE $criteria.In("ID",$RecordIDs.IDs) */)
#define($_ = $Status`, 1)
	writeSourceHandlerFile(t, root, "archive/business.go", factoryInputGraphBusiness)
	require.NoDirExists(t, filepath.Join(root, "archive/sql"))

	var first map[string]string
	for round := 0; round < 3; round++ {
		if round == 2 {
			_, err = db.Exec(`ALTER TABLE CHILD ADD COLUMN EXTRA TEXT`)
			require.NoError(t, err)
		}
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Settings.Mutation)
		require.Equal(t, "shape-discovery-only", compiled.Component.Settings.DefaultConnector)
		require.Len(t, compiled.Component.Views, 1)
		require.Len(t, compiled.ViewBindings, 1)
		shape := compiled.ExternalHandler.InputShape
		require.Len(t, shape.Relations, 1)
		var primaryKey bool
		for _, column := range shape.Columns {
			if column.Source == "ID" {
				primaryKey = column.PrimaryKey
				if mysql {
					require.True(t, column.AutoIncrement)
				}
			}
		}
		require.True(t, primaryKey)
		generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Nil(t, generated.Result.Plan.MutationHandler)
		require.Empty(t, generated.Result.Plan.RootSource)
		require.Equal(t, "shape-discovery-only", generated.Result.Plan.Connector)
		for _, name := range []string{"res.go", "child.go"} {
			data, err := os.ReadFile(filepath.Join(root, "archive", name))
			require.NoError(t, err)
			require.Regexp(t, `Has\s+\*`, string(data))
			require.Contains(t, string(data), "primaryKey")
			if name == "child.go" {
				require.Contains(t, string(data), "refTable=PARENT")
				require.Contains(t, string(data), "refColumn=ID")
				if mysql {
					require.NotContains(t, string(data), "migration_factory")
				}
			}
			require.NotContains(t, string(data), `view:"`)
			require.NotContains(t, string(data), `sql:"`)
			require.NotRegexp(t, `(?:^|[ \t\x60])on:"`, string(data))

			if mysql {
				require.Contains(t, string(data), "autoincrement")
			}
		}
		now := sourceHandlerSnapshot(t, root)
		if round == 0 {
			first = now
		} else if round == 1 {
			require.Equal(t, first, now)
		} else {
			require.NotEqual(t, first[filepath.Join(root, "archive/child.go")], now[filepath.Join(root, "archive/child.go")])
			require.Contains(t, now[filepath.Join(root, "archive/child.go")], "Extra")
		}
	}
	// A bad selected factory must not publish any changed Go or SQL resources.
	before := sourceHandlerSnapshot(t, root)
	business := before[filepath.Join(root, "archive/business.go")]
	require.Contains(t, business, "handler.Contract[ArchiveInput,ArchiveOutput]")
	writeSourceHandlerFile(t, root, "archive/business.go", strings.Replace(business, "handler.Contract[ArchiveInput,ArchiveOutput]", "handler.Contract[ArchiveOutput,ArchiveInput]", 1))
	invalidBefore := sourceHandlerSnapshot(t, root)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
	require.NoError(t, err)
	_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	require.Error(t, err)
	require.Equal(t, invalidBefore, sourceHandlerSnapshot(t, root))
	writeSourceHandlerFile(t, root, "archive/business.go", business)
	if !mysql {
		writeSourceHandlerFile(t, root, "archive/runtime_test.go", factoryInputGraphRuntime)
		cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "./archive")
		cmd.Dir = root
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		t.Logf("generated graph runtime:\n%s", output)
	}
}

func TestFactoryInputShapeReadOnlyCurrentWithoutBodyGraph(t *testing.T) {
	root, source := generatedPostFactoryFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "read.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)`)
	require.NoError(t, err)
	source.Connector = "read"
	source.ColumnRefiner = column.New(column.Connections{"read": db})
	// This fixture only needs a declared input read, so it has no body contract.
	source.Text = strings.Replace(source.Text, "#define($_ = $Data<*archive.ArchiveRequest>(body/data).Optional())", `#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CurrentRow') /* SELECT * FROM records */)`, 1)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
	require.NoError(t, err)
	require.Nil(t, compiled.Component.RootView)
	require.Nil(t, compiled.ExternalHandler.InputShape)
	require.Len(t, compiled.Component.Views, 1)
	require.Len(t, compiled.Component.Views[0].Columns, 2)
	require.Len(t, compiled.ViewBindings, 1)
}

const factoryInputGraphBusiness = `package archive
import("context";"fmt";"github.com/viant/xdatly/handler")
type OutputRes struct { TargetStr string; ExclusionStr string; Trusted map[string]int }
var observed *ArchiveInput
var calls int
type Handler struct{}
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(ctx context.Context,session handler.Session,in *ArchiveInput,out *ArchiveOutput)error{
 observed=in;calls++
 if len(in.Current)!=1 || in.Current[0].Target!="before" { return fmt.Errorf("Current was not bound from body keys before Exec") }
 value,found,err:=session.Binder().Lookup(ctx,handler.DMLKey);if err!=nil{return err};if !found{return fmt.Errorf("DML missing")};dml:=value.(handler.DML)
 sequence,found,err:=session.Binder().Lookup(ctx,handler.SequencerKey);if err!=nil{return err};if !found{return fmt.Errorf("sequencer missing")}
 row:=in.InpData[0]
 if err=dml.Update("PARENT",row);err!=nil{return err}
 if err=dml.Delete("CHILD",row.C);err!=nil{return err}
 fresh:=&Res{Target:"inserted",Exclusion:"ok",Has:&ResHas{Id:true,Target:true,Exclusion:true}}
 if err=sequence.(handler.Sequencer).Allocate(ctx,"PARENT",[]*Res{fresh},"Id");err!=nil{return err}
 if err=dml.Insert("PARENT",[]*Res{fresh});err!=nil{return err}
 if row.Exclusion=="fail"{return fmt.Errorf("deliberate custom failure")}
 if row.Exclusion=="duplicate" { if err=dml.Insert("PARENT",[]*Res{row});err!=nil{return err} }
 out.Status="ok";return nil
}
`

const factoryInputGraphRuntime = `package archive
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
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/handler/custom"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
)
func TestGeneratedFactoryGraph(t *testing.T){
 for _,mode:=range []string{"success","fail","duplicate"}{t.Run(mode,func(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t)
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE PARENT(ID INTEGER PRIMARY KEY AUTOINCREMENT,TARGET TEXT NOT NULL,EXCLUSION TEXT NOT NULL)","CREATE TABLE CHILD(ID INTEGER PRIMARY KEY AUTOINCREMENT,PARENT_ID INTEGER REFERENCES PARENT(ID),VALUE TEXT,EXTRA TEXT)","INSERT INTO PARENT VALUES(1,'before','original')","INSERT INTO PARENT VALUES(9,'decoy','original')","INSERT INTO CHILD VALUES(10,1,'before',NULL)"))
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok)
 metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 require.Nil(t,component.RootView);require.Len(t,component.Views,1);require.Empty(t,component.Settings.Mutation)
 resources:=resource.New();require.NoError(t,resources.Register(ArchiveDatlyResourceNamespace,ArchiveDatlyResources))
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[ArchiveInput](),OutputType:reflect.TypeFor[ArchiveOutput](),Resources:resources});require.NoError(t,err)
 require.Len(t,artifact.ViewDependencies,1)
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});require.NoError(t,err)
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[ArchiveOutput](),Handler:custom.New(NewArchive()),Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));require.NoError(t,err)
 exclusion:="ok";if mode!="success"{exclusion=mode}
 body:="{\"data\":[{\"id\":1,\"target\":\"after\",\"exclusion\":\""+exclusion+"\",\"c\":[{\"id\":10,\"value\":null}]}]}"
 request:=httptest.NewRequest("PATCH","/archive",strings.NewReader(body));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);require.NoError(t,err);defer scope.Close()
 before:=calls
 _,err=rt.ExecuteRoute(ctx,"PATCH","/archive",scope)
 if mode=="success"{require.NoError(t,err)}else{require.Error(t,err)}
 require.Equal(t,before+1,calls)
 require.NotNil(t,observed.Has);require.True(t,observed.Has.InpData)
 row:=observed.InpData[0];require.NotNil(t,row.Has);require.True(t,row.Has.Id);require.True(t,row.Has.Target);require.True(t,row.Has.C)
 require.NotNil(t,row.C[0].Has);require.True(t,row.C[0].Has.Value);require.Nil(t,row.C[0].Value);require.False(t,row.C[0].Has.ParentId)
 var target string;require.NoError(t,db.DB.QueryRow("SELECT TARGET FROM PARENT WHERE ID=1").Scan(&target))
 var parents,children int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM PARENT").Scan(&parents));require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM CHILD").Scan(&children))
 if mode=="success"{require.Equal(t,"after",target);require.Equal(t,3,parents);require.Zero(t,children)}else{require.Equal(t,"before",target);require.Equal(t,2,parents);require.Equal(t,1,children)}
 })}
}
`

func TestFactoryInputShapeAuxiliaryGraphInheritedPresence(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "aux.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE PARENT(ID INTEGER PRIMARY KEY,TARGET TEXT,EXCLUSION TEXT);CREATE TABLE CHILD(ID INTEGER PRIMARY KEY,PARENT_ID INTEGER,VALUE TEXT)`)
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,c.*,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION) FROM (PARENT) a JOIN (CHILD) c ON c.PARENT_ID=a.ID`, 1)
	var first map[string]string
	for round := 0; round < 2; round++ {
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Settings.Mutation)
		require.Empty(t, compiled.Component.Settings.DefaultConnector)
		generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Len(t, generated.Result.Plan.Views, 2)
		data, err := os.ReadFile(filepath.Join(root, "archive/res.go"))
		require.NoError(t, err)
		require.Contains(t, string(data), "type CView struct")
		require.Regexp(t, `C\s+bool`, string(data))
		require.Regexp(t, `Has\s+\*CViewHas`, string(data))
		now := sourceHandlerSnapshot(t, root)
		if round == 0 {
			first = now
		} else {
			require.Equal(t, first, now)
		}
	}
}
