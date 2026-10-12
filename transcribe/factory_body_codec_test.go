package transcribe

import (
	"context"
	"database/sql"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func factoryBodyCodecFixture(t *testing.T, one bool) (string, *Source, *sql.DB) {
	t.Helper()
	root, source := factoryInputShapeFixture(t)
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "codec.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`CREATE TABLE RECORD(ID INTEGER PRIMARY KEY,NAME TEXT NOT NULL,SECRET TEXT); CREATE TABLE CHILD(ID INTEGER PRIMARY KEY,RECORD_ID INTEGER REFERENCES RECORD(ID),NAME TEXT NOT NULL)`)
	require.NoError(t, err)
	source.ColumnRefiner = column.New(column.Connections{"shape-discovery-only": db})
	destination := "[]*archive.Record"
	if one {
		destination = "*" + handlerFixtureModule + "/archive.Record"
	}
	source.Text = strings.Replace(source.Text, "#import('archive',", "#import('json','encoding/json')\n#import('archive',", 1)
	source.Text = strings.Replace(source.Text, "$InpData<[]*Res>(body/data).Optional()", "$InpData<json.RawMessage,"+destination+">(body/data).WithCodec('archive.RecordJSON').Optional()", 1)
	source.Text = strings.Replace(source.Text, factoryInputProjection, `SELECT r.*, c.*, type(r,'Record'),dest(r,'record.go'),type(c,'Child'),dest(c,'child.go'),internal(r.SECRET) FROM (SELECT * FROM RECORD) r JOIN (SELECT * FROM CHILD) c ON c.RECORD_ID=r.ID`, 1)
	business := `package archive
import("context";"github.com/viant/xdatly/handler")
type OutputRes struct{ Name string }
type Handler struct{}
func NewArchive() handler.Contract[ArchiveInput,ArchiveOutput]{return &Handler{}}
func(*Handler)Exec(_ context.Context,_ handler.Session,_ *ArchiveInput,_ *ArchiveOutput)error{return nil}
`
	writeSourceHandlerFile(t, root, "archive/business.go", business)
	writeSourceHandlerFile(t, root, "archive/codec.go", factoryBodyCodecSource)
	return root, source, db
}

func TestFactoryBodyCodecNativeGeneration(t *testing.T) {
	for _, one := range []bool{false, true} {
		name := "many"
		if one {
			name = "one"
		}
		t.Run(name, func(t *testing.T) {
			root, source, db := factoryBodyCodecFixture(t, one)
			authored := source.Text
			var first map[string]string
			for round := 0; round < 3; round++ {
				if round == 2 {
					_, err := db.Exec(`ALTER TABLE CHILD ADD COLUMN EXTRA TEXT`)
					require.NoError(t, err)
				}
				compiled, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(t.Context(), source)
				require.NoError(t, err)
				require.Nil(t, compiled.Component.RootView)
				require.Empty(t, compiled.Component.Views)
				require.Empty(t, compiled.Component.Settings.Mutation)
				p := compiled.Component.Parameters[0]
				require.Equal(t, "json.RawMessage", p.TypeExpr)
				want := "[]*Record"
				cardinality := spec.CardinalityMany
				if one {
					want = "*Record"
					cardinality = spec.CardinalityOne
				}
				require.Equal(t, want, p.OutputTypeExpr)
				require.NotNil(t, p.Codec)
				require.Equal(t, handlerFixtureModule+"/archive.RecordJSON", p.Codec.Body)
				require.Equal(t, cardinality, compiled.ExternalHandler.InputShape.Cardinality)
				require.Len(t, compiled.ExternalHandler.InputShape.Relations, 1)
				generated, err := (Generator{Operation: "post"}).Generate(t.Context(), GenerationRequest{Compiled: compiled, Destination: root})
				require.NoError(t, err)
				require.Nil(t, generated.Result.Plan.MutationHandler)
				require.Empty(t, generated.Result.Plan.RootSource)
				require.Equal(t, authored, source.Text)
				snapshot := sourceHandlerSnapshot(t, root)
				if round == 0 {
					first = snapshot
				} else if round == 1 {
					require.Equal(t, first, snapshot)
				} else {
					require.NotEqual(t, first, snapshot)
					require.Contains(t, snapshot[filepath.Join(root, "archive/child.go")], "Extra")
				}
			}
			runtimeSource := factoryBodyCodecRuntime
			if one {
				runtimeSource = strings.ReplaceAll(runtimeSource, "[]*Record", "*Record")
				runtimeSource = strings.Replace(runtimeSource, "input.InpData[0]", "input.InpData", -1)
				runtimeSource = strings.Replace(runtimeSource, "`[{", "`{", -1)
				runtimeSource = strings.Replace(runtimeSource, "}]`", "}`", -1)
				runtimeSource = strings.Replace(runtimeSource, `bindRecord(t,"[]")`, `bindRecord(t,"{}")`, 1)
			}
			writeSourceHandlerFile(t, root, "archive/codec_runtime_test.go", runtimeSource)
			cmd := exec.CommandContext(t.Context(), "go", "test", "-mod=readonly", "-race", "-count=1", "-v", "-timeout=2m", "./archive")
			cmd.Dir = root
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", out)
			t.Logf("native codec binding:\n%s", out)
		})
	}
}

func TestFactoryBodyCodecFailuresDoNotPublish(t *testing.T) {
	for _, tc := range []struct{ name, old, replacement string }{
		{"missing output", "json.RawMessage,[]*archive.Record", "json.RawMessage"},
		{"missing codec", ".WithCodec('archive.RecordJSON')", ""},
		{"unrelated local", "json.RawMessage,[]*archive.Record", "json.RawMessage,[]*archive.OutputRes"},
		{"foreign destination", "json.RawMessage,[]*archive.Record", "json.RawMessage,[]*foreign.Record"},
		{"value destination", "json.RawMessage,[]*archive.Record", "json.RawMessage,archive.Record"},
		{"value slice", "json.RawMessage,[]*archive.Record", "json.RawMessage,[]archive.Record"},
		{"wrong wrapper", "json.RawMessage,[]*archive.Record", "json.RawMessage,**archive.Record"},
		{"emission", ".Optional()", ".Optional().Output()"},
		{"cardinality", ".WithCodec('archive.RecordJSON')", ".WithCodec('archive.RecordJSON').Cardinality('One')"},
		{"extra body", "#define($_ = $Status", "#define($_ = $Extra<json.RawMessage,[]*archive.Record>(body/extra).WithCodec('archive.RecordJSON'))\n#define($_ = $Status"},
		{"missing body", "(body/data)", "(param/data)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root, source, _ := factoryBodyCodecFixture(t, false)
			require.Contains(t, source.Text, tc.old)
			source.Text = strings.Replace(source.Text, tc.old, tc.replacement, 1)
			before := sourceHandlerSnapshot(t, root)
			_, err := (&Discovery{BaseDir: root, GoBuild: source.GoBuild, ColumnRefiner: source.ColumnRefiner}).CompileSource(context.Background(), source)
			require.Error(t, err)
			require.Equal(t, before, sourceHandlerSnapshot(t, root))
			t.Log(err)
		})
	}
}

const factoryBodyCodecSource = `package archive
import("context";"encoding/json";"fmt";"reflect";"github.com/viant/xdatly/codec")
type RecordJSON struct{}
var _anchorRecordJSON=reflect.TypeFor[RecordJSON]()
func(RecordJSON)New(c *codec.Config,_ ...codec.Option)(codec.Instance,error){
 if c==nil||c.SourceType!=reflect.TypeFor[json.RawMessage]()||(c.DestinationType!=reflect.TypeFor[[]*Record]()&&c.DestinationType!=reflect.TypeFor[*Record]()){return nil,fmt.Errorf("incompatible record codec contract")}
 return recordJSONInstance{one:c.DestinationType==reflect.TypeFor[*Record]()},nil
}
type recordJSONInstance struct{one bool}
func(c recordJSONInstance)Value(ctx context.Context,v any,_ ...codec.Option)(any,error){
 if err:=ctx.Err();err!=nil{return nil,err}
 if v==nil{if c.one{return (*Record)(nil),nil};return ([]*Record)(nil),nil}
 raw,ok:=v.(json.RawMessage);if !ok{return nil,fmt.Errorf("expected RawMessage")}
 if c.one{var row *Record;err:=json.Unmarshal(raw,&row);return row,err}
 var rows []*Record;err:=json.Unmarshal(raw,&rows);return rows,err
}
func normalizeName(raw []byte)([]byte,map[string]json.RawMessage,error){
 var fields map[string]json.RawMessage
 if err:=json.Unmarshal(raw,&fields);err!=nil{return nil,nil,err}
 if _,ok:=fields["name"];!ok{if value,present:=fields["label"];present{fields["name"]=value}}
 delete(fields,"Has");delete(fields,"has");delete(fields,"secret")
 normalized,err:=json.Marshal(fields);return normalized,fields,err
}
func(r *Record)UnmarshalJSON(raw []byte)error{
 normalized,fields,err:=normalizeName(raw);if err!=nil{return err};type plain Record
 if err=json.Unmarshal(normalized,(*plain)(r));err!=nil{return err}
 r.Has=&RecordHas{};_,r.Has.Name=fields["name"];_,r.Has.C=fields["c"];return nil
}
func(c *Child)UnmarshalJSON(raw []byte)error{
 normalized,fields,err:=normalizeName(raw);if err!=nil{return err};type plain Child
 if err=json.Unmarshal(normalized,(*plain)(c));err!=nil{return err}
 c.Has=&ChildHas{};_,c.Has.Name=fields["name"];return nil
}
`

const factoryBodyCodecRuntime = `package archive
import("encoding/json";"fmt";"reflect";"testing";
"github.com/stretchr/testify/require";"github.com/viant/bindly";"github.com/viant/bindly/provider/body";
"github.com/viant/datly/spec";"github.com/viant/datly/bootstrap";dtag "github.com/viant/datly/tag";"github.com/viant/datly/runtime/handler/compiler";"github.com/viant/xdatly/codec")
func bindRecord(t *testing.T,raw string)(*ArchiveInput,error){
 t.Helper(); provider,err:=body.New([]byte("{\"data\":"+raw+"}"),"application/json",nil);require.NoError(t,err)
 holder:=reflect.TypeFor[ArchiveComponent]();field,ok:=holder.FieldByName("Contract");require.True(t,ok);metadata,present,err:=dtag.ParseComponent(field.Tag);require.NoError(t,err);require.True(t,present)
 component,err:=(&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"archive",PackagePath:holder.PkgPath(),Tag:metadata,InputType:"ArchiveInput",OutputType:"ArchiveOutput"}).Resolve(reflect.TypeFor[ArchiveInput](),reflect.TypeFor[ArchiveOutput]());require.NoError(t,err)
 require.Nil(t,component.RootView);require.Empty(t,component.Views);require.Empty(t,component.Settings.Mutation)
 require.NotNil(t,component.Parameters[0].Codec);require.Equal(t,"github.com/viant/datly/handlerfixture/archive.RecordJSON",component.Parameters[0].Codec.Body)
 binding:=compiler.New(compiler.Input{Component:component,InputType:reflect.TypeFor[ArchiveInput](),CodecFactory:RecordJSON{},TypeLookup:func(name string)(reflect.Type,error){if name=="json.RawMessage"{return reflect.TypeFor[json.RawMessage](),nil};return nil,fmt.Errorf("unknown source %s",name)}})
 bindings,err:=binding.CompileBindings();require.NoError(t,err);require.Len(t,bindings,1);require.Equal(t,reflect.TypeFor[json.RawMessage](),bindings[0].SourceType);require.NotNil(t,bindings[0].Transformer)
 injector,err:=bindly.NewInjector(bindly.WithProviders(provider));require.NoError(t,err);plan,err:=injector.CompilePlan(reflect.TypeFor[ArchiveInput](),bindings...);require.NoError(t,err)
 input:=&ArchiveInput{};err=injector.Bind(t.Context(),input,bindly.WithPlan(plan),bindly.WithSource(input));return input,err
}
func TestNativeRecordCodec(t *testing.T){
 input,err:=bindRecord(t,` + "`" + `[{"label":"root","secret":"forged","Has":{"Secret":true},"c":[{"label":"child","Has":{"Name":false}}]}]` + "`" + `);require.NoError(t,err)
 row:=input.InpData[0];require.NotNil(t,row.Name);require.Equal(t,"root",*row.Name);require.True(t,row.Has.Name);require.True(t,row.Has.C);require.False(t,row.Has.Secret);require.Nil(t,row.Secret);require.Len(t,row.C,1);require.NotNil(t,row.C[0].Name);require.Equal(t,"child",*row.C[0].Name);require.True(t,row.C[0].Has.Name)
 _,err=bindRecord(t,` + "`" + `[{"name":4}]` + "`" + `);require.Error(t,err)
 nullInput,err:=bindRecord(t,"null");require.NoError(t,err);require.Nil(t,nullInput.InpData)
 emptyInput,err:=bindRecord(t,"[]");require.NoError(t,err);require.NotNil(t,emptyInput.InpData)
 factory:=RecordJSON{}
 for _,config:=range []*codec.Config{nil,{SourceType:reflect.TypeFor[string](),DestinationType:reflect.TypeFor[[]*Record]()},{SourceType:reflect.TypeFor[json.RawMessage](),DestinationType:reflect.TypeFor[[]*Child]()}}{_,err:=factory.New(config);require.Error(t,err)}
 // Existing compiler must reject an unresolved source or absent codec factory.
 component:=&spec.Component{Parameters:[]*spec.Parameter{{Name:"InpData",TypeExpr:"missing.Raw",Source:spec.BindSource{Kind:"body",Name:"data"},Codec:&spec.Codec{Body:"RecordJSON"}}}}
 _,err=compiler.New(compiler.Input{Component:component,InputType:reflect.TypeFor[ArchiveInput](),CodecFactory:factory,TypeLookup:func(string)(reflect.Type,error){return nil,fmt.Errorf("source unavailable")}}).CompileBindings();require.Error(t,err)
 component.Parameters[0].TypeExpr="string"
 _,err=compiler.New(compiler.Input{Component:component,InputType:reflect.TypeFor[ArchiveInput](),CodecFactory:factory,TypeLookup:func(string)(reflect.Type,error){return reflect.TypeFor[string](),nil}}).CompileBindings();require.Error(t,err)
 component.Parameters[0].TypeExpr="json.RawMessage"
 type mismatchedInput struct{InpData []*Child}
 _,err=compiler.New(compiler.Input{Component:component,InputType:reflect.TypeFor[mismatchedInput](),CodecFactory:factory,TypeLookup:func(string)(reflect.Type,error){return reflect.TypeFor[json.RawMessage](),nil}}).CompileBindings();require.Error(t,err)
 _,err=compiler.New(compiler.Input{Component:component,InputType:reflect.TypeFor[ArchiveInput]()}).CompileBindings();require.Error(t,err)

}
`
