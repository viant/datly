package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
)

func TestFactoryForwardGeneratedHelperNativeGeneration(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	source.Text = strings.Replace(source.Text, "#setting($_ = $internal(true))\n", "", 1)
	business, err := os.ReadFile(filepath.Join(root, "archive/business.go"))
	require.NoError(t, err)
	code := strings.Replace(string(business), "observed=in;", "observed=in;receivedDictionary=in.InpData[0].Dictionary;in.InpData[0].Dictionary=in.Current[0];", 1)
	code += "\nvar receivedDictionary *Dictionary\nfunc(*Handler) RequiresReadMetadata()bool{return true}\n"
	writeSourceHandlerFile(t, root, "archive/business.go", code)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE RECORDS(TARGET TEXT,EXCLUSION TEXT)", "CREATE TABLE dictionaries(id INTEGER PRIMARY KEY,caption TEXT)"))
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db.DB})
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,type(a,'archive.Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),CAST(a.Dictionary AS '*archive.Dictionary'),tag(a.Dictionary,'sqlx:"-"') FROM (SELECT *,NULL AS Dictionary FROM RECORDS) a`, 1)
	source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $Current<?>(view/Current).Cardinality('Many').WithTypeName('archive.Dictionary') /* SELECT d.*,type(d,'archive.Dictionary') FROM dictionaries d */)
#define($_ = $Status`, 1)
	var stable map[string]string
	for round := 0; round < 3; round++ {
		if round == 2 {
			require.NoError(t, db.ExecStatements(context.Background(), "ALTER TABLE dictionaries ADD visible INTEGER"))
		}
		compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
		require.NoError(t, err)
		require.Len(t, compiled.Component.Views, 1)
		require.Equal(t, "SELECT d.* FROM dictionaries d", strings.Join(strings.Fields(compiled.Component.Views[0].Source.SQL), " "), "Current executable SQL must retain its original wildcard and alias")
		generated, err := (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
		require.NoError(t, err)
		require.Nil(t, generated.Result.Plan.MutationHandler, "factory remains the sole persistence owner")
		now := sourceHandlerSnapshot(t, root)
		if round == 0 {
			stable = now
		} else if round == 1 {
			require.Equal(t, stable, now, "regeneration must be stable with existing artifacts")
		}
		if round == 2 {
			found := false
			for _, view := range generated.Result.Plan.Views {
				if view.Name != "Dictionary" {
					continue
				}
				for _, field := range view.Fields {
					if field.Name == "Visible" {
						found = true
					}
				}
			}
			require.True(t, found, "fresh database schema must supersede the old generated helper")
		}
	}
	writeSourceHandlerFile(t, root, "archive/helper_runtime_test.go", forwardHelperRuntime)
	cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "./archive")
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s (package %s)", output, filepath.Join(root, "archive"))
}

func TestFactoryForwardHelperPreservesAuthoredType(t *testing.T) {
	root, source := factoryInputShapeFixture(t)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(context.Background(), "CREATE TABLE RECORDS(TARGET TEXT,EXCLUSION TEXT)", "CREATE TABLE dictionaries(id INTEGER PRIMARY KEY,caption TEXT)"))
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db.DB})
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT a.*,type(a,'archive.Res'),dest(a,'res.go'),required(a.TARGET),required(a.EXCLUSION),CAST(a.Dictionary AS '*archive.Dictionary'),tag(a.Dictionary,'sqlx:"-"') FROM (SELECT *,NULL AS Dictionary FROM RECORDS) a`, 1)
	source.Text = strings.Replace(source.Text, "#define($_ = $Status", `#define($_ = $Current<?>(view/Current).Cardinality('Many').WithTypeName('archive.Dictionary') /* SELECT d.*,type(d,'archive.Dictionary') FROM dictionaries d */)
#define($_ = $Status`, 1)
	writeSourceHandlerFile(t, root, "archive/dictionary_authored.go", `package archive
type Dictionary struct { Caption string }
`)
	before := sourceHandlerSnapshot(t, root)
	compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
	if err == nil {
		_, err = (Generator{Operation: "post"}).Generate(context.Background(), GenerationRequest{Compiled: compiled, Destination: root})
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "Dictionary")
	require.Equal(t, before, sourceHandlerSnapshot(t, root), "failed generation must not replace authored authority or publish partial artifacts")
}

const forwardHelperRuntime = `package archive
import (
 "context"
 stdjson "encoding/json"
 json "github.com/viant/structology/encoding/json"
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
func TestHelperActualCurrentReadAndSparsePresence(t *testing.T) {
 ctx:=context.Background();db:=sqlite.New(t)
 require.NoError(t,db.ExecStatements(ctx,"CREATE TABLE RECORDS(TARGET TEXT,EXCLUSION TEXT)","CREATE TABLE dictionaries(id INTEGER PRIMARY KEY,caption TEXT,visible INTEGER)","INSERT INTO dictionaries VALUES(1,'database',1)"))
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok)
 metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 require.Nil(t,component.RootView);require.Len(t,component.Views,1);require.Empty(t,component.Settings.Mutation)
 resources:=resource.New();require.NoError(t,resources.Register(ArchiveDatlyResourceNamespace,ArchiveDatlyResources))
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[ArchiveInput](),OutputType:reflect.TypeFor[ArchiveOutput](),Resources:resources});require.NoError(t,err)
 require.Len(t,artifact.ViewDependencies,1)
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});require.NoError(t,err)
 rt,err:=druntime.NewRuntime([]*druntime.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[ArchiveOutput](),Handler:custom.New(NewArchive()),Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));require.NoError(t,err)
 request:=httptest.NewRequest("PATCH","/archive",strings.NewReader("{\"data\":[{\"Target\":\"request\",\"Exclusion\":\"ok\",\"Dictionary\":{\"Caption\":\"body\"}}]}"));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);require.NoError(t,err);defer scope.Close()
 _,err=rt.ExecuteRoute(ctx,"PATCH","/archive",scope);require.NoError(t,err)
 require.NotNil(t,receivedDictionary);require.NotNil(t,receivedDictionary.Has)
 require.True(t,receivedDictionary.Has.Caption);require.False(t,receivedDictionary.Has.Visible)
 require.Len(t,observed.Current,1);current:=observed.Current[0]
 require.Nil(t,current.Has,"database hydration must not invent client presence")
 require.NotNil(t,current.Caption);require.Equal(t,"database",*current.Caption)
 require.Same(t,current,observed.InpData[0].Dictionary)
 wire,err:=stdjson.Marshal(current);require.NoError(t,err)
 nativeWire,err:=json.Marshal(current);require.NoError(t,err)
 require.JSONEq(t,string(wire),string(nativeWire));require.NotContains(t,string(wire),"Has")
 var document map[string]any;require.NoError(t,stdjson.Unmarshal(wire,&document));require.Equal(t,"database",document["Caption"])
 var count int;require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM dictionaries WHERE id=1 AND caption='database' AND visible=1").Scan(&count));require.Equal(t,1,count)
 require.NoError(t,db.DB.QueryRow("SELECT COUNT(*) FROM RECORDS").Scan(&count));require.Zero(t,count,"custom factory must not acquire automatic persistence")
}
`
