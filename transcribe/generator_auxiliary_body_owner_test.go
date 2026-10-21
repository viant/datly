package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

const auxiliaryOwnerDQL = `#package('github.com/viant/datly/auxownerfixture/generated')
#import('model','github.com/viant/datly/auxownerfixture/model')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Records<[]*Record>(body/data).Cardinality('Many'))
#define($_ = $Data<[]*Record>(output/body).Cardinality('Many'))
#define($_ = $Current<[]*Record>(view/Current).Cardinality('Many').TypeName('Record') /* SELECT id,type_id FROM records WHERE id=1 */)
#define($_ = $Candidates<[]*Record>(view/Candidates).Cardinality('Many').TypeName('Record') /* SELECT id,type_id FROM records WHERE id=2 */)
SELECT r.*,type(r,'Record'),lifecycle_type(r,'BatchLifecycle'),
 CAST(r.id AS int),r.type_id AS Kind,CAST(r.type_id AS int),
 CAST(r.note AS string),tag(r.note,'sqlx:"-"'),
 CAST(r.caller AS '*model.Caller'),tag(r.caller,'sqlx:"-" json:"-"')
FROM (SELECT id,type_id,NULL AS note,NULL AS caller FROM (records)) r`

func auxiliaryOwnerFixture(t *testing.T, text string) (string, *Source) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/auxownerfixture"}).Write(t, root)
	writeSourceFile(t, root, "model/caller.go", "package model\ntype Caller struct { Name string }\n")
	pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	db := sqlite.New(t)
	if err = db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER)"); err != nil {
		t.Fatal(err)
	}
	return root, &Source{Name: "records", Scope: "github.com/viant/datly/auxownerfixture/source", Connector: "main", Text: text, Types: catalog, ColumnRefiner: column.New(column.Connections{"main": db.DB})}
}

func TestGeneratorAuxiliaryCanonicalBodyOwner(t *testing.T) {
	root, source := auxiliaryOwnerFixture(t, auxiliaryOwnerDQL)
	var first map[string]string
	for pass := 0; pass < 3; pass++ {
		generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		owners, shared := 0, 0
		for _, view := range generated.Result.Plan.Views {
			if view.Name != "Record" {
				continue
			}
			switch view.Ownership {
			case gen.ViewGenerated:
				owners++
				if len(view.SetMarkerFields) == 0 {
					t.Fatal("body Has owner missing")
				}
			case gen.ViewSharedBody:
				shared++
				if len(view.Fields) != 0 || view.SharedBodyIdentity == "" {
					t.Fatal("read gained ownership")
				}
			}
		}
		if owners != 1 || shared != 2 {
			t.Fatalf("owners=%d shared=%d", owners, shared)
		}
		if pass == 0 {
			writeSourceFile(t, root, "generated/lifecycle.go", auxiliaryOwnerHooks)
			continue
		}
		snapshot := map[string]string{}
		for _, file := range generated.Result.Files {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			snapshot[file.Path] = string(data)
		}
		if pass == 1 {
			first = snapshot
		} else if !reflect.DeepEqual(first, snapshot) {
			t.Fatal("repeat artifact drift")
		}
	}
	if source.Text != auxiliaryOwnerDQL {
		t.Fatal("source mutated")
	}
	for _, sql := range []string{"SELECT id,type_id FROM records WHERE id=1", "SELECT id,type_id FROM records WHERE id=2"} {
		found := false
		for path, data := range first {
			if strings.HasSuffix(path, ".sql") && strings.TrimSpace(data) == sql {
				found = true
			}
		}
		if !found {
			t.Fatal("independent SQL changed", sql)
		}
	}
	writeSourceFile(t, root, "generated/auxiliary_owner_runtime_test.go", auxiliaryOwnerRuntime)
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated module: %v\n%s", err, output)
	}
}

const auxiliaryOwnerHooks = `package generated
import("context";"fmt";h "github.com/viant/xdatly/handler")
var ownerInitCalls,ownerValidateCalls int
type BatchLifecycle struct{}
func(*BatchLifecycle)Init(ctx context.Context,input *Input,state h.LifecycleContext[Input,h.NoParent,Output])error{
 ownerInitCalls++
 if state.Previous!=nil||state.Parent!=nil||state.Original!=nil||state.PreviousFields!=nil {return fmt.Errorf("not a canonical Input lifecycle")}
 if len(input.Current)!=1||input.Current[0].Id!=1||input.Current[0].Kind!=7||len(input.Candidates)!=1||input.Candidates[0].Id!=2||input.Candidates[0].Kind!=8 {return fmt.Errorf("raw read alias/cast mapping lost")}
 metadata,ok:=h.ReadMetadataFromContext(ctx);if !ok{return fmt.Errorf("read evidence missing")}
 for _,name:=range []string{"Current","Candidates"}{projection,err:=metadata.Projection(name);if err!=nil{return err};fields,err:=projection.Fields(0);if err!=nil{return err};if !fields.Has("Id")||!fields.Has("Kind")||fields.Has("Note")||fields.Has("Caller")||fields.Has("Has"){return fmt.Errorf("read loaded transient or lost physical mapping: %s",name)}}
 for _,row:=range append(append([]*Record{},input.Current...),input.Candidates...){if row.Has!=nil||row.Note!=""||row.Caller!=nil{return fmt.Errorf("read synthesized body-only state")}}
 if len(input.Records)!=1||input.Records[0].Has==nil||!input.Records[0].Has.Note||!input.Records[0].Has.Kind||input.Records[0].Has.Caller{return fmt.Errorf("body presence changed")}
 state.Output.Data=input.Records
 return nil
}
func(*BatchLifecycle)Validate(context.Context,*Input,h.LifecycleContext[Input,h.NoParent,Output])error{ownerValidateCalls++;return nil}
`

const auxiliaryOwnerRuntime = `package generated
import (
 "context"
 "database/sql"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func TestGeneratedAuxiliaryOwner(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 for _,query:=range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER)","INSERT INTO records VALUES(1,7),(2,8)","CREATE TRIGGER no_write BEFORE UPDATE ON records BEGIN SELECT RAISE(ABORT,'auxiliary write');END"}{if _,err=db.Exec(query);err!=nil{t.Fatal(err)}}
 for _,name:=range []string{"Records","Current","Candidates"}{field,ok:=reflect.TypeFor[Input]().FieldByName(name);if !ok||field.Type!=reflect.TypeFor[[]*Record](){t.Fatalf("public row type changed: %s",name)}}
 for _,name:=range []string{"Note","Caller"}{field,ok:=reflect.TypeFor[Record]().FieldByName(name);if !ok||field.Tag.Get("sqlx")!="-"||field.Tag.Get("view")!=""||field.Tag.Get("on")!=""{t.Fatalf("transient became relation: %s",name)}}
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 metadata,err:=writer.Compile(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 if !metadata.Root.Auxiliary||len(metadata.Root.Relations)!=0{t.Fatal("not a true auxiliary leaf")}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:native,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 request:=httptest.NewRequest("PATCH","/records",strings.NewReader("{\"data\":[{\"id\":1,\"kind\":99,\"note\":\"request-only\"}]}"));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope);if err!=nil{t.Fatal(err)}
 if ownerInitCalls!=1||ownerValidateCalls!=1||len(output.(*Output).Data)!=1||output.(*Output).Data[0].Note!="request-only"{t.Fatal("canonical lifecycle/output lost")}
 var count,total int;if err=db.QueryRow("SELECT COUNT(*),SUM(type_id) FROM records").Scan(&count,&total);err!=nil||count!=2||total!=15{t.Fatalf("auxiliary DML: %d %d %v",count,total,err)}
 if err=db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count);err!=nil||count!=1{t.Fatalf("auxiliary allocated storage: %d %v",count,err)}
}
`

func TestGeneratorAuxiliaryCanonicalBodyOwnerRejections(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		edit       func(string) string
		badHook    bool
		operation  string
	}{
		{name: "missing declaration", want: "map to type", edit: func(s string) string { return strings.Replace(s, ",lifecycle_type(r,'BatchLifecycle')", "", 1) }},
		{name: "foreign missing hook", want: "cannot scaffold package", edit: func(s string) string {
			return strings.Replace(s, "lifecycle_type(r,'BatchLifecycle')", "lifecycle_type(r,'example.com/missing.BatchLifecycle')", 1)
		}},
		{name: "bad canonical Input signature", want: "incompatible signature", badHook: true},
		{name: "ordinary physical root unchanged", want: "map to type", edit: func(s string) string { return strings.Replace(s, "FROM (records)) r", "FROM records) r", 1) }},
		{name: "different canonical read type", want: "does not match package type", edit: func(s string) string { return strings.Replace(s, "TypeName('Record')", "TypeName('Other')", 1) }},
		{name: "different destination", want: "destination", edit: func(s string) string {
			return strings.Replace(s, "TypeName('Record')", "TypeName('Record').Dest('current.go')", 1)
		}},
		{name: "unmapped read column", want: "no SQLX field", edit: func(s string) string {
			return strings.Replace(s, "SELECT id,type_id FROM records WHERE id=1", "SELECT id,type_id,100 AS extra FROM records WHERE id=1", 1)
		}},
		{name: "explicit incompatible CAST", want: "conflicts with body", edit: func(s string) string {
			return strings.Replace(s, "SELECT id,type_id FROM records WHERE id=1", "SELECT current.*,CAST(current.type_id AS string) FROM (SELECT id,type_id FROM records WHERE id=1) current", 1)
		}},
		{name: "nonmutation target", operation: "get", want: "requires the generated Go mutation lifecycle", edit: func(s string) string { return strings.Replace(s, "'/records','PATCH'", "'/records','GET'", 1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := auxiliaryOwnerDQL
			if tc.edit != nil {
				text = tc.edit(text)
			}
			root, source := auxiliaryOwnerFixture(t, text)
			if tc.badHook {
				authored := `package hooks
import("context";h "github.com/viant/xdatly/handler")
type OtherInput struct{}
type OtherOutput struct{}
type WrongLifecycle struct{}
func(*WrongLifecycle)Init(context.Context,*OtherInput,h.LifecycleContext[OtherInput,h.NoParent,OtherOutput])error{return nil}
func(*WrongLifecycle)Validate(context.Context,*OtherInput,h.LifecycleContext[OtherInput,h.NoParent,OtherOutput])error{return nil}
`
				writeSourceFile(t, root, "hooks/lifecycle.go", authored)
				pkg, err := loaderast.LoadPackageFS(t.Context(), os.DirFS(root), "hooks")
				if err != nil {
					t.Fatal(err)
				}
				if err = source.Types.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
					t.Fatal(err)
				}
				source.Text = strings.Replace(source.Text, "#import('model'", "#import('hooks','github.com/viant/datly/auxownerfixture/hooks')\n#import('model'", 1)
				source.Text = strings.Replace(source.Text, "lifecycle_type(r,'BatchLifecycle')", "lifecycle_type(r,'hooks.WrongLifecycle')", 1)
			}
			operation := tc.operation
			if operation == "" {
				operation = "patch"
			}
			original := source.Text
			_, err := (Generator{Operation: operation}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
			if source.Text != original {
				t.Fatal("rejected source changed")
			}
			if _, statErr := os.Stat(filepath.Join(root, "generated", "router.go")); !os.IsNotExist(statErr) {
				t.Fatalf("rejected source emitted route: %v", statErr)
			}
			t.Logf("rejected: %v", err)
		})
	}
}

func TestGeneratorAuxiliaryUnsharedKeylessBodyKeepsPresence(t *testing.T) {
	const text = `#package('github.com/viant/datly/auxownerfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Records<*Request>(body/data))
#define($_ = $Data<*Request>(output/body))
#define($_ = $Evidence<?>(view/Evidence).Cardinality('Many').TypeName('Evidence') /* SELECT id,type_id FROM records */)
SELECT r.*,type(r,'Request'),CAST(r.IDS AS []int),CAST(r.CAMPAIGN_ID AS int),lifecycle_type(r,'BatchLifecycle')
FROM (SELECT json_group_array(id) AS IDS,0 AS CAMPAIGN_ID FROM (records)) r`
	root, source := auxiliaryOwnerFixture(t, text)
	generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	bodyFound, evidenceFound := false, false
	for _, view := range generated.Result.Plan.Views {
		switch view.Name {
		case "Request":
			bodyFound = true
			// Standalone lifecycle commands still distinguish omitted fields from
			// explicit empty/zero values, independently of read/body type sharing.
			if !reflect.DeepEqual(view.SetMarkerFields, []string{"Ids", "CampaignId"}) {
				t.Fatalf("keyless command presence changed: %v", view.SetMarkerFields)
			}
		case "Evidence":
			evidenceFound = true
			if len(view.SetMarkerFields) != 0 {
				t.Fatal("independent evidence read gained body presence")
			}
		}
	}
	if !bodyFound || !evidenceFound {
		t.Fatalf("missing body or evidence plan: body=%t evidence=%t", bodyFound, evidenceFound)
	}
	markers := 0
	for _, file := range generated.Result.Files {
		if strings.HasSuffix(file.Path, ".go") {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			markers += strings.Count(string(data), "type RequestHas struct")
		}
	}
	if markers != 1 {
		t.Fatalf("expected one command presence type, got %d", markers)
	}
}

func TestGeneratorAuxiliaryCanonicalBodyOwnerPendingSource(t *testing.T) {
	for _, physical := range []bool{false, true} {
		name := "auxiliary"
		table := "(records)"
		if physical {
			name = "physical source remains rejected"
			table = "records"
		}
		t.Run(name, func(t *testing.T) {
			text := strings.Replace(auxiliaryOwnerDQL, "SELECT id,type_id,NULL AS note,NULL AS caller FROM (records)", "${embed:sql/body.sql}", 1)
			root, source := auxiliaryOwnerFixture(t, text)
			source.Resources = resource.New()
			if err := source.Resources.Register("", fstest.MapFS{"sql/body.sql": &fstest.MapFile{Data: []byte("SELECT id,type_id,NULL AS note,NULL AS caller FROM " + table)}}); err != nil {
				t.Fatal(err)
			}
			generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
			if physical {
				if err == nil {
					t.Fatal("pending recognition admitted final physical source")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			owners, shared := 0, 0
			for _, view := range generated.Result.Plan.Views {
				if view.Name == "Record" {
					if view.Ownership == gen.ViewGenerated {
						owners++
					}
					if view.Ownership == gen.ViewSharedBody {
						shared++
					}
				}
			}
			if owners != 1 || shared != 2 {
				t.Fatalf("owners=%d shared=%d", owners, shared)
			}
		})
	}
}
