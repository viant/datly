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
	handlerplan "github.com/viant/datly/transcribe/handler/ast"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
)

// A custom persistence owner uses a generated body graph and independently
// declared reads. The graph supplies types and presence, never a reader root.
func TestFactoryInputShapeJoinedCurrentGeneration(t *testing.T) {
	testFactoryInputShapeJoinedCurrentGeneration(t, false)
}

func TestFactoryInputShapeSharedCurrentGeneration(t *testing.T) {
	testFactoryInputShapeJoinedCurrentGeneration(t, true)
}

func TestFactoryInputShapeSharedTransientCurrentGeneration(t *testing.T) {
	testFactoryInputShapeJoinedCurrentGeneration(t, true, true)
}

func testFactoryInputShapeJoinedCurrentGeneration(t *testing.T, shared bool, transient ...bool) {
	excluded := len(transient) > 0 && transient[0]
	t.Helper()
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
	schema := []string{`CREATE TABLE PARENT(ID INTEGER PRIMARY KEY AUTOINCREMENT,TARGET TEXT NOT NULL,EXCLUSION TEXT NOT NULL)`, `CREATE TABLE CHILD(ID INTEGER PRIMARY KEY AUTOINCREMENT,PARENT_ID INTEGER REFERENCES PARENT(ID),VALUE TEXT)`, `CREATE TABLE NOTE(ID INTEGER PRIMARY KEY,PARENT_ID INTEGER REFERENCES PARENT(ID),VALUE TEXT)`}
	if mysql {
		schema = []string{`CREATE TABLE PARENT(ID BIGINT PRIMARY KEY AUTO_INCREMENT,TARGET VARCHAR(255) NOT NULL,EXCLUSION VARCHAR(255) NOT NULL) ENGINE=InnoDB`, `CREATE TABLE CHILD(ID BIGINT PRIMARY KEY AUTO_INCREMENT,PARENT_ID BIGINT,VALUE VARCHAR(255),FOREIGN KEY(PARENT_ID) REFERENCES PARENT(ID)) ENGINE=InnoDB`, `CREATE TABLE NOTE(ID BIGINT PRIMARY KEY,PARENT_ID BIGINT,VALUE VARCHAR(255),FOREIGN KEY(PARENT_ID) REFERENCES PARENT(ID)) ENGINE=InnoDB`}
	}
	if shared {
		for i := range schema {
			if strings.HasPrefix(schema[i], "CREATE TABLE PARENT(") {
				schema[i] = strings.Replace(schema[i], "EXCLUSION TEXT NOT NULL)", "EXCLUSION TEXT NOT NULL,SECRET TEXT)", 1)
				schema[i] = strings.Replace(schema[i], "EXCLUSION VARCHAR(255) NOT NULL)", "EXCLUSION VARCHAR(255) NOT NULL,SECRET VARCHAR(255))", 1)
			}
		}
	}
	for _, statement := range schema {
		_, err = db.Exec(statement)
		require.NoError(t, err)
	}
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*, c.*, n.*, type(n,'Note'),type(a,'Res'), dest(a,'res.go'), type(c,'Child'), dest(c,'child.go'), required(a.TARGET), required(a.EXCLUSION) FROM (SELECT * FROM PARENT) a JOIN (SELECT * FROM CHILD) c ON c.PARENT_ID=a.ID LEFT JOIN (SELECT * FROM NOTE) n ON n.PARENT_ID=a.ID AND 1=1`, 1)
	source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $RecordIDs<?>(param/InpData) /* ? SELECT ARRAY_AGG(Id) AS IDs FROM `+"`/`"+` LIMIT 1 */)
#define($_ = $Current<?>(view/Current).Cardinality('Many').TypeName('CurrentRow').Dest('current.go') /* SELECT p.*,CAST(p.TARGET AS string) FROM PARENT p WHERE $criteria.In("ID",$RecordIDs.IDs) */)
#define($_ = $Status`, 1)
	businessSource := factoryInputGraphBusiness
	if shared {
		source.Text = strings.Replace(source.Text, "FROM (SELECT * FROM PARENT) a", "FROM (SELECT *, '' AS LOGIC FROM PARENT) a", 1)
		source.Text = strings.Replace(source.Text, "required(a.EXCLUSION) FROM", `required(a.EXCLUSION), tag(a.SECRET,'internal:"true" json:"-"'), internal_transient(a.LOGIC) FROM`, 1)
		source.Text = strings.Replace(source.Text, "TypeName('CurrentRow').Dest('current.go')", "TypeName('archive.Res').Dest('res.go')", 1)
		source.Text = strings.ReplaceAll(source.Text, "'ArchiveInput'", "'archive.ArchiveInput'")
		source.Text = strings.ReplaceAll(source.Text, "'ArchiveOutput'", "'archive.ArchiveOutput'")
		source.Text = strings.ReplaceAll(source.Text, "[]*Res>", "[]*archive.Res>")
		source.Text = strings.ReplaceAll(source.Text, "type(a,'Res')", "type(a,'archive.Res')")
		source.Text = strings.ReplaceAll(source.Text, "type(c,'Child')", "type(c,'archive.Child')")
		source.Text = strings.ReplaceAll(source.Text, "type(n,'Note')", "type(n,'archive.Note')")
		source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $OtherCurrent<?>(view/OtherCurrent).Cardinality('Many').TypeName('archive.Res').Dest('res.go') /* SELECT p.* FROM PARENT p WHERE ID=9 */)
#define($_ = $CurrentNote<?>(view/CurrentNote).Cardinality('Many').TypeName('archive.Note').Dest('res.go') /* SELECT n.* FROM NOTE n WHERE ID=11 */)
#define($_ = $Status`, 1)
		businessSource = strings.ReplaceAll(businessSource, "CurrentRow", "Res")
		businessSource = strings.Replace(businessSource, "observedIndex=current", `observedIndex=current
 if current.Has != nil { return fmt.Errorf("Current falsely marked request presence") }
 if current.Secret == nil || *current.Secret != "server-only" { return fmt.Errorf("private persisted field was not read") }
 if in.InpData[0].Secret != nil || in.InpData[0].Logic != "" { return fmt.Errorf("private or transient field widened request binding") }
 if len(in.OtherCurrent)!=1 || in.OtherCurrent[0].Target!="decoy" { return fmt.Errorf("independent Current SQL lost") }
 if current.C != nil || current.N != nil { return fmt.Errorf("Current loaded body-only relations") }
 if len(in.CurrentNote)!=1 || in.CurrentNote[0].Has != nil { return fmt.Errorf("shared descendant lost read identity or presence") }
 oldNote:=in.InpData[0].N
 in.InpData[0].N=in.CurrentNote[0]
 if in.InpData[0].N!=in.CurrentNote[0] { return fmt.Errorf("descendant pointer identity lost") }
 in.CurrentNote[0].SetValue(in.CurrentNote[0].Value)
 in.InpData[0].N=oldNote
 bodyRows := append(in.InpData, current)
 if bodyRows[len(bodyRows)-1] != current { return fmt.Errorf("Current body pointer identity lost") }
 current.SetTarget("before")
 if current.Has == nil || !current.Has.Target { return fmt.Errorf("shared Current methods unavailable") }`, 1)
	}
	if excluded {
		source.Text = strings.Replace(source.Text, `tag(a.SECRET,'internal:"true" json:"-"')`, `internal_transient(a.SECRET)`, 1)
		businessSource = strings.Replace(businessSource, `if current.Secret == nil || *current.Secret != "server-only" { return fmt.Errorf("private persisted field was not read") }`, `if current.Secret != nil || in.OtherCurrent[0].Secret != nil || in.OtherCurrent[0].Has != nil { return fmt.Errorf("transient physical Current leaked values or presence") }`, 1)
	}
	// MySQL reports the primary key as non-nullable; SQLite's scan metadata
	// supplies a pointer. Exercise each discovered contract without forcing
	// the SQLite fixture's pointer into the MySQL generated row.
	if mysql {
		businessSource = strings.ReplaceAll(businessSource, "*in.Current[0].Id", "in.Current[0].Id")
	}
	writeSourceHandlerFile(t, root, "archive/business.go", businessSource)
	require.NoDirExists(t, filepath.Join(root, "archive/sql"))

	var first map[string]string
	for round := 0; round < 3; round++ {
		if round == 2 {
			_, err = db.Exec(`ALTER TABLE CHILD ADD COLUMN EXTRA TEXT`)
			require.NoError(t, err)
			if shared {
				_, err = db.Exec(`ALTER TABLE PARENT ADD COLUMN REVISION TEXT`)
				require.NoError(t, err)
			}
		}
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Nil(t, compiled.Component.RootView)
		require.Empty(t, compiled.Component.Settings.Mutation)
		require.Equal(t, "shape-discovery-only", compiled.Component.Settings.DefaultConnector)
		expectedReads := 1
		if shared {
			expectedReads = 3
		}
		require.Len(t, compiled.Component.Views, expectedReads)
		require.Len(t, compiled.ViewBindings, expectedReads)
		shape := compiled.ExternalHandler.InputShape
		require.Len(t, shape.Relations, 2)
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
		require.NotNil(t, generated.Result.Plan.EntitySupport)
		require.NotNil(t, generated.Result.Plan.ReadIndexes)
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
		if shared {
			declarations := map[string]int{}
			for file, contents := range now {
				if filepath.Ext(file) != ".go" {
					continue
				}
				for _, name := range []string{"Res", "ResHas", "Note", "NoteHas"} {
					declarations[name] += strings.Count(contents, "type "+name+" struct")
				}
			}
			for name, count := range declarations {
				require.Equal(t, 1, count, "one body-owned declaration for %s", name)
			}
		}
		if round == 0 {
			first = now
		} else if round == 1 {
			require.Equal(t, first, now)
		} else {
			require.NotEqual(t, first[filepath.Join(root, "archive/child.go")], now[filepath.Join(root, "archive/child.go")])
			require.Contains(t, now[filepath.Join(root, "archive/child.go")], "Extra")
			if shared {
				require.NotEqual(t, first[filepath.Join(root, "archive/res.go")], now[filepath.Join(root, "archive/res.go")])
				require.Contains(t, now[filepath.Join(root, "archive/res.go")], "Revision")
				var generated string
				for _, contents := range now {
					generated += contents
				}
				require.Contains(t, generated, "SetRevision")
			}
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
		runtimeSource := factoryInputGraphRuntime
		if shared {
			runtimeSource = strings.ReplaceAll(runtimeSource, "EXCLUSION TEXT NOT NULL)", "EXCLUSION TEXT NOT NULL,SECRET TEXT,REVISION TEXT)")
			runtimeSource = strings.ReplaceAll(runtimeSource, "INSERT INTO PARENT VALUES", "INSERT INTO PARENT(ID,TARGET,EXCLUSION) VALUES")
			runtimeSource = strings.Replace(runtimeSource, `"INSERT INTO NOTE VALUES(11,1,'before')"`, `"INSERT INTO NOTE VALUES(11,1,'before')","UPDATE PARENT SET SECRET='server-only'"`, 1)
			runtimeSource = strings.Replace(runtimeSource, `\"target\":\"after\"`, `\"target\":\"after\",\"secret\":\"untrusted\",\"logic\":\"untrusted\"`, 1)
			runtimeSource = strings.ReplaceAll(runtimeSource, "require.Len(t,component.Views,1)", "require.Len(t,component.Views,3)")
			runtimeSource = strings.ReplaceAll(runtimeSource, "require.Len(t,artifact.ViewDependencies,1)", "require.Len(t,artifact.ViewDependencies,3)")
		}
		if excluded {
			runtimeSource = strings.Replace(runtimeSource, "UPDATE PARENT SET SECRET='server-only'", "UPDATE PARENT SET SECRET='server-only' WHERE ID=1", 1)
			runtimeSource = strings.Replace(runtimeSource, "var notes int;", `secretField,_:=reflect.TypeFor[Res]().FieldByName("Secret");require.Equal(t,"-",secretField.Tag.Get("sqlx"));require.Equal(t,"-",secretField.Tag.Get("json"));require.Equal(t,"-",secretField.Tag.Get("diff"))
 var secret string;require.NoError(t,db.DB.QueryRow("SELECT SECRET FROM PARENT WHERE ID=1").Scan(&secret));require.Equal(t,"server-only",secret)
 var notes int;`, 1)
		}
		writeSourceHandlerFile(t, root, "archive/runtime_test.go", runtimeSource)
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
var observedIndex *CurrentRow
type Handler struct{}
func(*Handler) RequiresReadMetadata()bool{return true}
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput] { return &Handler{} }
func (*Handler) Exec(ctx context.Context,session handler.Session,in *ArchiveInput,out *ArchiveOutput)error{
 observed=in;calls++
 if err:=in.PrepareReadIndexes(ctx);err!=nil{return err}
 indexes,err:=in.ReadIndexes(ctx);if err!=nil{return err}
 if len(in.Current)!=1{return fmt.Errorf("missing Current")}
 current:=indexes.CurrentById[*in.Current[0].Id]
 if current==nil || current.Target!="before" {return fmt.Errorf("read indexes lost bound metadata")}
 if current==observedIndex{return fmt.Errorf("cached read indexes did not refresh")}
 observedIndex=current
 originalTarget:=in.InpData[0].Target
 in.InpData[0].SetTarget("")
 if !in.InpData[0].Has.Target || in.InpData[0].Target!="" {return fmt.Errorf("zero setter lost presence")}
 in.InpData[0].SetTarget(originalTarget)
 for _,child:=range in.InpData[0].C{child.SetValue(nil);if !child.Has.Value || child.Value!=nil{return fmt.Errorf("null setter lost presence")}}
 in.InpData[0].N.SetValue(nil)
 if len(in.Current)!=1 || in.Current[0].Target!="before" { return fmt.Errorf("Current was not bound from body keys before Exec") }
 value,found,err:=session.Binder().Lookup(ctx,handler.DMLKey);if err!=nil{return err};if !found{return fmt.Errorf("DML missing")};dml:=value.(handler.DML)
 sequence,found,err:=session.Binder().Lookup(ctx,handler.SequencerKey);if err!=nil{return err};if !found{return fmt.Errorf("sequencer missing")}
 row:=in.InpData[0]
 if err=dml.Update("PARENT",row);err!=nil{return err}
 if err=dml.Delete("CHILD",row.C);err!=nil{return err}
 if err=dml.Delete("NOTE",row.N);err!=nil{return err}
 fresh:=&Res{Target:"inserted",Exclusion:"ok",N:row.N,Has:&ResHas{Id:true,Target:true,Exclusion:true}}
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
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"
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
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE PARENT(ID INTEGER PRIMARY KEY AUTOINCREMENT,TARGET TEXT NOT NULL,EXCLUSION TEXT NOT NULL)","CREATE TABLE CHILD(ID INTEGER PRIMARY KEY AUTOINCREMENT,PARENT_ID INTEGER REFERENCES PARENT(ID),VALUE TEXT,EXTRA TEXT)","INSERT INTO PARENT VALUES(1,'before','original')","INSERT INTO PARENT VALUES(9,'decoy','original')","INSERT INTO CHILD VALUES(10,1,'before',NULL)","CREATE TABLE NOTE(ID INTEGER PRIMARY KEY,PARENT_ID INTEGER REFERENCES PARENT(ID),VALUE TEXT)","INSERT INTO NOTE VALUES(11,1,'before')"))
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
 body:="{\"data\":[{\"id\":1,\"target\":\"after\",\"exclusion\":\""+exclusion+"\",\"c\":[{\"id\":10,\"value\":null}],\"n\":{\"id\":11,\"value\":null}}]}"
 request:=httptest.NewRequest("PATCH","/archive",strings.NewReader(body));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);require.NoError(t,err);defer scope.Close()
 before:=calls
 _,err=rt.ExecuteRoute(ctx,"PATCH","/archive",scope)
 if mode=="success"{require.NoError(t,err)}else{require.Error(t,err)}
 require.Equal(t,before+1,calls)
 require.NotNil(t,observed.Has);require.True(t,observed.Has.InpData)
 row:=observed.InpData[0];require.NotNil(t,row.Has);require.True(t,row.Has.Id);require.True(t,row.Has.Target);require.True(t,row.Has.C)
 require.NotNil(t,row.C[0].Has);require.True(t,row.C[0].Has.Value);require.Nil(t,row.C[0].Value);require.False(t,row.C[0].Has.ParentId)
 require.NotNil(t,row.N.Has);require.True(t,row.N.Has.Value);require.Nil(t,row.N.Value)
 noteField,_:=reflect.TypeFor[Res]().FieldByName("N");require.Equal(t,"-",noteField.Tag.Get("sqlx"));require.Empty(t,noteField.Tag.Get("view"));require.Empty(t,noteField.Tag.Get("on"))
 targetField,_:=reflect.TypeFor[Res]().FieldByName("Target");require.Equal(t,"TARGET,required=true",targetField.Tag.Get("sqlx"))
 var notes int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM NOTE").Scan(&notes));if mode=="success"{require.Zero(t,notes)}else{require.Equal(t,1,notes)}
 var target string;require.NoError(t,db.DB.QueryRow("SELECT TARGET FROM PARENT WHERE ID=1").Scan(&target))
 var parents,children int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM PARENT").Scan(&parents));require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM CHILD").Scan(&children))
 if mode=="success"{require.Equal(t,"after",target);require.Equal(t,3,parents);require.Zero(t,children)}else{require.Equal(t,"before",target);require.Equal(t,2,parents);require.Equal(t,1,children)}
 if mode=="success"{
  shared:=observed
  previous:=observedIndex
  require.NoError(t,db.ExecStatements(ctx,"UPDATE PARENT SET TARGET='before' WHERE ID=1","INSERT INTO CHILD VALUES(10,1,'before',NULL)","INSERT INTO NOTE VALUES(11,1,'before')"))
  before=calls
  _,err=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:spec.Key{Kind:spec.KindComponent,Scope:"github.com/viant/datly/handlerfixture/archive",Name:"Archive"},Route:spec.RouteRef{Method:"PATCH",Path:"/archive"}},Input:shared})
  require.NoError(t,err)
  require.Same(t,shared,observed)
  require.Equal(t,before+1,calls)
  require.NotSame(t,previous,observedIndex,"read indexes must use fresh metadata on reused typed input")
  require.NoError(t,db.DB.QueryRow("SELECT TARGET FROM PARENT WHERE ID=1").Scan(&target));require.Equal(t,"after",target)
  require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM PARENT").Scan(&parents));require.Equal(t,4,parents,"only the custom persistence owner inserts")
  require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM CHILD").Scan(&children));require.Zero(t,children)
 }

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

// Execution policy belongs to the custom factory, while generated helpers
// retain the complete SQL-derived body, including auxiliary control fields.
func TestFactoryInputShapeCustomAuxiliaryMarkerCompanions(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "markers.db"))
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE PARENT(ID INTEGER PRIMARY KEY,TARGET TEXT,EXCLUSION TEXT);CREATE TABLE CHILD(ID INTEGER PRIMARY KEY,PARENT_ID INTEGER,VALUE TEXT,DELETED BOOLEAN,REVISION INTEGER)`)
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,c.*,type(a,'Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),CAST(c.DELETED AS bool),delete_marker(c.DELETED),CAST(c.REVISION AS int),concurrency_token(c.REVISION) FROM (PARENT) a JOIN (CHILD) c ON c.PARENT_ID=a.ID`, 1)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
	require.NoError(t, err)
	child := compiled.ExternalHandler.InputShape.Relations[0].View
	var marker bool
	for _, col := range child.Columns {
		marker = marker || col.DeleteMarker
	}
	require.True(t, marker)
	var token bool
	for _, col := range child.Columns {
		token = token || col.ConcurrencyToken
	}
	require.True(t, token)
	automatic := compiled.Component.Clone()
	automatic.RootView = compiled.ExternalHandler.InputShape.Clone()
	_, err = (&handlercompiler.Compiler{}).Compile(handlercompiler.Request{Component: automatic, ViewBindings: compiled.ViewBindings, Operation: handlerplan.OperationPost})
	require.ErrorContains(t, err, "mutation marker")
	generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	require.NoError(t, err)
	require.Nil(t, generated.Result.Plan.MutationHandler)
	require.Empty(t, compiled.Component.Settings.Mutation)
	require.Nil(t, compiled.Component.RootView)
	var retained bool
	for _, col := range child.Columns {
		retained = retained || col.DeleteMarker
	}
	require.True(t, retained, "helper preparation must not mutate canonical policy")
	var retainedToken bool
	for _, col := range child.Columns {
		retainedToken = retainedToken || col.ConcurrencyToken
	}
	require.True(t, retainedToken)
	snapshot := sourceHandlerSnapshot(t, root)
	_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	require.NoError(t, err)
	require.Equal(t, snapshot, sourceHandlerSnapshot(t, root))
	writeSourceHandlerFile(t, root, "archive/companions_test.go", `package archive
import("testing";"reflect";"github.com/stretchr/testify/require")
func TestAuxiliarySetters(t *testing.T){
 row:=&Res{}
 row.SetTarget("")
 require.NotNil(t,row.Has);require.True(t,row.Has.Target)
 leaf:=&CView{}
 leaf.SetDeleted(true);require.True(t,leaf.Deleted);require.True(t,leaf.Has.Deleted)
 leaf.SetDeleted(false);leaf.SetValue(nil);leaf.SetRevision(0)
 typ:=reflect.TypeFor[CView]()
 deleted,_:=typ.FieldByName("Deleted");require.Equal(t,"delete",deleted.Tag.Get("writer"));require.Equal(t,"-",deleted.Tag.Get("sqlx"))
 revision,_:=typ.FieldByName("Revision");require.Equal(t,"concurrency",revision.Tag.Get("writer"));require.True(t,leaf.Has.Revision)
 require.NotNil(t,leaf.Has);require.True(t,leaf.Has.Deleted);require.False(t,leaf.Deleted)
 require.True(t,leaf.Has.Value);require.Nil(t,leaf.Value)
}
`)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
