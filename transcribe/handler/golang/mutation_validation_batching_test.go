package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedFrameworkValidationBatchesUniqueCandidatesSQLite(t *testing.T) {
	for _, tc := range []struct {
		name          string
		semantic      *plan.Plan
		types         []RecordType
		events        string
		wantLocations string
		wantHooks     string
	}{
		{
			name:          "same role",
			semantic:      sameRoleValidationBatchSemantic(),
			types:         rootRecordTypes(sameRoleValidationBatchSemantic(), "[]*Record", ""),
			events:        `[]*Record{first,second}`,
			wantLocations: `[]string{"Input.Events[0].Name","Input.Events[1].Name"}`,
			wantHooks:     `[2]int{2,0}`,
		},
		{
			name:          "reused role",
			semantic:      reusedRoleValidationBatchSemantic(),
			types:         reusedRoleValidationBatchTypes(reusedRoleValidationBatchSemantic()),
			events:        `[]*Record{first};first.Children=[]*Record{second}`,
			wantLocations: `[]string{"Input.Events[0].Name","Input.Events.Children[0].Name"}`,
			wantHooks:     `[2]int{1,1}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			asset, err := MutationProgram(tc.semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: tc.types})
			if err != nil {
				t.Fatal(err)
			}
			files, err := asset.Files()
			if err != nil {
				t.Fatal(err)
			}
			var products []*ast.File
			for _, file := range files {
				if file != asset.Entities.File {
					products = append(products, file)
				}
			}
			source := strings.NewReplacer(
				"{{FACTORY}}", asset.Factory,
				"{{DEFINITION}}", asset.Definition,
				"{{EVENTS}}", tc.events,
				"{{LOCATIONS}}", tc.wantLocations,
				"{{HOOKS}}", tc.wantHooks,
			).Replace(validationBatchSQLiteFixture)
			(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
		})
	}
}

func sameRoleValidationBatchSemantic() *plan.Plan {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Table = "records"
	semantic.Root.Sequence = nil
	semantic.Root.Entity = validationBatchEntity(true)
	semantic.Root.Entity.Hooks = spec.TypeRef{Name: "Hooks"}
	semantic.Root.Entity.HooksBind = true
	return semantic
}

func reusedRoleValidationBatchSemantic() *plan.Plan {
	semantic := sameRoleValidationBatchSemantic()
	root := semantic.Root
	child := &plan.RecordPlan{
		Identity:    "view::EventChildren|namespace:",
		InputPath:   plan.FieldPath{"Input", "Events", "Children"},
		Table:       root.Table,
		Cardinality: spec.CardinalityMany,
		Keys:        cloneFixtureKeys(root.Keys),
		Write:       fixtureWritePolicy(plan.OperationPost, plan.FieldPath{"Input", "Events", "Children"}, 1),
		Entity:      validationBatchEntity(false),
	}
	child.Entity.Hooks = spec.TypeRef{Name: "ChildHooks"}
	child.Entity.HooksBind = true
	root.Relations = []*plan.RelationPlan{{
		Identity:    "Children",
		FieldPath:   plan.FieldPath{"Children"},
		Cardinality: spec.CardinalityMany,
		Links: []plan.KeyLink{{
			Parent:     root.Keys[0],
			Child:      plan.KeyPart{Field: "ParentId", Source: "PARENT_ID", Type: spec.TypeRef{Name: "int64", Pointer: true}},
			Conversion: plan.LinkDirect,
		}},
		Child: child,
	}}
	return semantic
}

func validationBatchEntity(withChildren bool) *plan.EntityPlan {
	fields := []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "ParentId", Type: spec.TypeRef{Name: "*int64"}, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
	}
	if withChildren {
		fields = append(fields, plan.EntityField{Name: "Children", Type: spec.TypeRef{Name: "[]*Record", Cardinality: spec.CardinalityMany}, Relation: true, Writable: true})
	}
	return &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: []plan.KeyPart{{Field: "Id", Source: "ID", Type: spec.TypeRef{Name: "int64", Pointer: true}}}, Fields: fields}
}

func reusedRoleValidationBatchTypes(semantic *plan.Plan) []RecordType {
	return []RecordType{
		{Identity: semantic.Root.Identity, Path: semantic.Root.InputPath, Value: "[]*Record"},
		{Identity: semantic.Root.Relations[0].Child.Identity, Path: semantic.Root.Relations[0].Child.InputPath, Value: "[]*Record"},
	}
}

const validationBatchSQLiteFixture = `package events
import(
 "context";"errors";"reflect";"sort";"testing";"time"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/handler/mutation";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";"github.com/viant/datly/sql/dml"
 "github.com/viant/xdatly/handler"
)
type Marker struct{Id,ParentId,Name,Children bool}
type Record struct{Id *int64 ` + "`sqlx:\"id,primaryKey\"`" + `;ParentId *int64 ` + "`sqlx:\"parent_id\"`" + `;Name string ` + "`sqlx:\"name,unique,table=records\"`" + `;Children []*Record ` + "`sqlx:\"-\"`" + `;Has *Marker ` + "`setMarker:\"true\" sqlx:\"-\"`" + `}
type Input struct{Events []*Record}
type Output struct{Data []*Record}
var initialized,validated,queued [2]int
type Hooks struct{}
func(*Hooks)Init(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{initialized[0]++;return nil}
func(*Hooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{validated[0]++;return nil}
func(*Hooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{queued[0]++;return nil}
type ChildHooks struct{}
func(*ChildHooks)Init(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{initialized[1]++;return nil}
func(*ChildHooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{validated[1]++;return nil}
func(*ChildHooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{queued[1]++;return nil}
func TestValidationBatch(t *testing.T){
 for _,duplicate:=range []bool{true,false}{t.Run(map[bool]string{true:"duplicate",false:"distinct"}[duplicate],func(t *testing.T){
  initialized=[2]int{};validated=[2]int{};queued=[2]int{}
  ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();h:=sqlite.New(t)
  // Keep total_changes() on the same connection, including across rollback.
  h.DB.SetMaxOpenConns(1);h.DB.SetMaxIdleConns(1)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT UNIQUE)");err!=nil{t.Fatal(err)}
  bindings:=[]bindly.BindingSpec{{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}}}
  seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
  bound,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=bound.Projection();if err!=nil{t.Fatal(err)}
  ref:=spec.RouteRef{Method:"POST",Path:"/records"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:bound,Bindings:bindings});if err!=nil{t.Fatal(err)}
  route,ok:=contract.ForRoute(ref);if !ok{t.Fatal("route missing")}
  one,two:=int64(1),int64(2)
  first:=&Record{Id:&one,Name:"same",Has:&Marker{Id:true,Name:true}}
  second:=&Record{Id:&two,Name:"same",Has:&Marker{Id:true,Name:true}}
  if !duplicate{second.Name="different"}
  events:={{EVENTS}}
  definition:={{FACTORY}}().(*{{DEFINITION}})
  _,err=engine.New().Execute(ctx,engine.Request{Input:route,Handler:mutation.New[Input,Output](definition),DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{values.New("test",map[string]any{"events":events})}})
  wantHooks:={{HOOKS}}
  if initialized!=wantHooks{t.Fatalf("entity Init calls=%v want=%v error=%v",initialized,wantHooks,err)}
  wantWrites:=0
  if duplicate{
   var failed *handler.Validation
   if !errors.As(err,&failed)||!failed.Failed||len(failed.Violations)!=2{t.Fatalf("expected two framework UNIQUE violations, got %v",err)}
   locations:=make([]string,0,len(failed.Violations))
   for _,violation:=range failed.Violations{
    if violation.Check!="unique"||violation.Field!="Name"{t.Fatalf("unexpected violation: %+v",violation)}
    locations=append(locations,violation.Location)
   }
   wantLocations:={{LOCATIONS}};sort.Strings(locations);sort.Strings(wantLocations)
   if !reflect.DeepEqual(locations,wantLocations){t.Fatalf("locations=%v want=%v",locations,wantLocations)}
   if validated!=[2]int{}||queued!=[2]int{}{t.Fatalf("validation failure reached custom hooks: Validate=%v AfterQueue=%v",validated,queued)}
  }else{
   if err!=nil{t.Fatal(err)}
   if validated!=wantHooks||queued!=wantHooks{t.Fatalf("successful controls did not run hooks: Validate=%v AfterQueue=%v want=%v",validated,queued,wantHooks)}
   wantWrites=2
  }
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM records"},[]struct{N int}{{wantWrites}})
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT total_changes() AS n"},[]struct{N int}{{wantWrites}})
 })}
}
`
