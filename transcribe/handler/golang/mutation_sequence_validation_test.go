package golang

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

type sequenceValidationFixture struct{ reused, nonKey bool }

func (f sequenceValidationFixture) semantic() *plan.Plan {
	semantic := (partialIdentityFixture{operation: plan.OperationPost}).semantic()
	root := semantic.Root
	if f.nonKey {
		root.Keys = cloneFixtureKeys(root.Keys[1:])
		root.Entity.Keys = cloneFixtureKeys(root.Keys)
		root.Entity.Fields[0].Identity = false
	}
	root.Entity.Fields = append(root.Entity.Fields,
		plan.EntityField{Name: "ParentId", Type: spec.TypeRef{Name: "*int64"}, Writable: true},
		plan.EntityField{Name: "Context", Type: spec.TypeRef{Name: "string"}},
		plan.EntityField{Name: "Details", Type: spec.TypeRef{Name: "*LogicalContext"}},
		plan.EntityField{Name: "Links", Type: spec.TypeRef{Name: "[]*LogicalContext"}, Relation: true},
		plan.EntityField{Name: "Labels", Type: spec.TypeRef{Name: "[]string"}, Relation: true},
		plan.EntityField{Name: "Children", Type: spec.TypeRef{Name: "[]*Record"}, Relation: true, Writable: true},
	)
	if f.reused {
		child := &plan.RecordPlan{Identity: "view::Children|namespace:", InputPath: plan.FieldPath{"Input", "Events", "Children"}, Table: root.Table, Cardinality: spec.CardinalityMany, Keys: cloneFixtureKeys(root.Keys), Write: fixtureWritePolicy(plan.OperationPost, plan.FieldPath{"Input", "Events", "Children"}, 1)}
		entity := *root.Entity
		entity.Hooks = spec.TypeRef{Name: "ChildHooks"}
		child.Entity = &entity
		root.Relations = []*plan.RelationPlan{{Identity: "Children", FieldPath: plan.FieldPath{"Children"}, Cardinality: spec.CardinalityMany, Links: []plan.KeyLink{{Parent: root.Keys[0], Child: plan.KeyPart{Field: "ParentId", Source: "PARENT_ID", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}}, Child: child}}
	}
	return semantic
}

func TestGeneratedSequenceBusinessAndFinalValidationSQLite(t *testing.T) {
	for _, f := range []sequenceValidationFixture{{}, {reused: true}, {nonKey: true}} {
		name := "same role"
		if f.reused {
			name = "reused role"
		}
		if f.nonKey {
			name = "non-key sequence field"
		}
		t.Run(name, func(t *testing.T) {
			semantic := f.semantic()
			config := (partialIdentityFixture{operation: plan.OperationPost}).config(semantic)
			if f.reused {
				child := semantic.Root.Relations[0].Child
				config.Records = append(config.Records, RecordType{Identity: child.Identity, Path: child.InputPath, Value: "[]*Record"})
			}
			asset, err := MutationProgram(semantic, config)
			if err != nil {
				t.Fatal(err)
			}
			files, err := asset.Files()
			if err != nil {
				t.Fatal(err)
			}
			events, secondLocation := "[]*Record{first,second}", "Input.Events[1].Id"
			if f.reused {
				events, secondLocation = "[]*Record{first};first.Children=[]*Record{second}", "Input.Events.Children[0].Id"
			}
			source := strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition, "{{PROGRAM}}", asset.Program, "{{EVENTS}}", events, "{{SECOND_LOCATION}}", secondLocation).Replace(sequenceValidationSQLiteFixture)
			if f.nonKey {
				source = strings.Replace(source, `"relation missing","initialized valid key","custom value"`, `"relation missing","custom value"`, 1)
				source = strings.Replace(source, `"pending changed key",`, "", 1)
				source = strings.NewReplacer("id,primaryKey,unique,table=records", "id,unique,table=records", "PRIMARY KEY(tenant_id,id)", "PRIMARY KEY(tenant_id)").Replace(source)
			}
			// This test validates generated sequencing semantics in three fresh
			// modules. Race instrumentation is covered by the shared entity-sync
			// acceptance fixtures; omitting it here avoids three redundant,
			// memory-heavy race builds during the full repository suite.
			(entitySyncFixture{entity: asset.Entities, products: files, source: source, withoutRace: true}).run(t)
		})
	}
}

func (f sequenceValidationFixture) update() (*plan.Plan, Config) {
	semantic := f.semantic()
	semantic.Operation = plan.OperationPatch
	root, child := semantic.Root, semantic.Root.Relations[0].Child
	// These authored final-link validation cases explicitly request reparenting.
	root.Relations[0].AllowReparent = true
	root.Sequence = nil
	for index, record := range []*plan.RecordPlan{root, child} {
		record.Write = fixtureWritePolicy(plan.OperationPatch, record.InputPath, index)
		name := "CurrentEvents"
		if index == 1 {
			name = "CurrentChildren"
		}
		record.Current = &plan.CurrentPlan{ParamIdentity: name, ViewIdentity: "view::" + name + "|namespace:", InputPath: plan.FieldPath{"Input", name}, Keys: cloneFixtureKeys(record.Keys)}
		for _, field := range record.Entity.Fields {
			if !field.Relation && field.Writable {
				record.Current.Fields = append(record.Current.Fields, plan.CurrentField{Current: plan.FieldRef{Field: field.Name, Type: field.Type}, Entity: plan.FieldRef{Field: field.Name, Type: field.Type}, Conversion: plan.LinkDirect})
			}
		}
	}
	config := (partialIdentityFixture{operation: plan.OperationPatch}).config(semantic)
	config.Records = append(config.Records, RecordType{Identity: child.Identity, Path: child.InputPath, Value: "[]*Record", Current: "[]*Previous"})
	return semantic, config
}

func TestGeneratedFinalReconciledUpdateCoverageSQLite(t *testing.T) {
	semantic, config := (sequenceValidationFixture{reused: true}).update()
	asset, err := MutationProgram(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	source := strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(finalUpdateCoverageSQLiteFixture)
	(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
}

func TestGeneratedPreviousEvidenceImmutabilitySQLite(t *testing.T) {
	for _, mode := range []string{"control", "custom prior", "native business prior", "native final prior", "init prior", "after sequence prior", "custom evidence", "native business evidence", "native final evidence"} {
		t.Run(mode, func(t *testing.T) {
			semantic, config := (sequenceValidationFixture{reused: true}).update()
			asset, err := MutationProgram(semantic, config)
			if err != nil {
				t.Fatal(err)
			}
			files, err := asset.Files()
			if err != nil {
				t.Fatal(err)
			}
			source := strings.NewReplacer(
				"{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition,
				`"github.com/viant/xdatly/handler"`, `"github.com/viant/xdatly/handler";policy "github.com/viant/xdatly/handler/mutation";"github.com/viant/govalidator";"strings"`,
				`definition:={{FACTORY}}().(*{{DEFINITION}})`, `definition:=&observedDefinition{`+asset.Factory+`().(*`+asset.Definition+`)}`,
				` validate:"gt(50)"`, ``,
				`sqlx:"name"`, `sqlx:"name,unique,uniqueDep=parent_id,table=records"`,
				`sqlx:"tenant_id,primaryKey"`, `sqlx:"tenant_id,primaryKey" validate:"prior_probe"`,
				`name TEXT,PRIMARY KEY(tenant_id,id)`, `name TEXT,UNIQUE(parent_id,name),PRIMARY KEY(tenant_id,id)`,
				`(7,20,99,'child')`, `(7,20,99,'child'),(9,30,10,'child')`,
				`Name:"changed child",Has:&Marker{Id:true,TenantId:true,Name:true}`, `Name:"",Has:&Marker{Id:true,TenantId:true,Name:false}`,
				`var customCalls,queueCalls int`, `var customCalls,queueCalls int
var childPrevious *Record
var childEvidence *mutableEvidence
var finalPhase bool
const priorMode="`+mode+`"
type mutableEvidence struct{handler.FieldSet;hideName bool}
func(m *mutableEvidence)Has(name string)bool{return !(m.hideName&&name=="Name")&&m.FieldSet.Has(name)}
type observedDefinition struct{*`+asset.Definition+`}
func(d *observedDefinition)Capture(ctx context.Context,input *Input)(policy.Program[Output],error){
 program,err:=d.`+asset.Definition+`.Capture(ctx,input);if err!=nil{return program,err}
 captured:=program.(*`+asset.Program+`)
 for _,entry:=range captured.database.`+asset.Frames.Previous.Roles[1].TypeName+`.byKey{if *entry.value.Id==20{childEvidence=&mutableEvidence{FieldSet:entry.fields};entry.fields=childEvidence}}
 return program,nil
}
func init(){govalidator.RegisterWithDependencies("prior_probe",func(*govalidator.Field,*govalidator.Check)(govalidator.IsValid,error){return func(_ context.Context,value any)(bool,error){
 if value==int64(7)&&((!finalPhase&&priorMode=="native business prior")||(finalPhase&&priorMode=="native final prior")){childPrevious.Name="fake"}
 if value==int64(7)&&((!finalPhase&&priorMode=="native business evidence")||(finalPhase&&priorMode=="native final evidence")){childEvidence.hideName=true}
 return true,nil
},nil},func(*govalidator.Field,*govalidator.Check)([]string,error){return nil,nil})}
func(*Hooks)AfterSequence(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{finalPhase=true;if priorMode=="after sequence prior"{childPrevious.Name="fake"};return nil}`,
				`func(*ChildHooks)Init(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{return nil}`, `func(*ChildHooks)Init(_ context.Context,_ *Record,state handler.LifecycleContext[Record,Record,Output])error{childPrevious=state.Previous;if priorMode=="init prior"{childPrevious.Name="fake"};return nil}`,
				`func(*ChildHooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{customCalls++;return nil}`, `func(*ChildHooks)Validate(_ context.Context,_ *Record,state handler.LifecycleContext[Record,Record,Output])error{customCalls++;if priorMode=="custom prior"{state.Previous.Name="fake"};if priorMode=="custom evidence"{childEvidence.hideName=true};return nil}`,
				`var failed *handler.Validation`, `if priorMode!="control"{
 if err==nil||!strings.Contains(err.Error(),"immutable Previous"){t.Fatalf("prior mutation escaped guard: %v custom=%d queued=%d",err,customCalls,queueCalls)}
 if queueCalls!=0{t.Fatal("corrupt Previous reached Queue")}
 if (priorMode=="init prior"||strings.HasPrefix(priorMode,"native business"))&&customCalls!=0{t.Fatal("corrupt Previous reached custom validation")}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name,parent_id FROM records ORDER BY id"},[]struct{Name string;ParentId int64}{{"parent",99},{"child",99},{"child",10}})
 return
}
var failed *handler.Validation`,
				`v.Field!="ParentId"||v.Location!="Input.Events.Children[0].ParentId"`, `v.Field!="Name"||v.Check!="unique"||v.Location!="Input.Events.Children[0].Name"`,
				`{{"parent",99},{"child",99}}`, `{{"parent",99},{"child",99},{"child",10}}`,
			).Replace(finalUpdateCoverageSQLiteFixture)
			if strings.HasSuffix(mode, "evidence") {
				source = strings.NewReplacer("(9,30,10,'child')", "(9,30,10,'other')", `{"child",10}`, `{"other",10}`).Replace(source)
			}
			(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
		})
	}
}

const finalUpdateCoverageSQLiteFixture = `package events
import(
 "context";"errors";"reflect";"testing"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/handler/mutation";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";"github.com/viant/datly/sql/reader/compiler";viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
)
type Marker struct{Id,TenantId,ParentId,Name,Context,Details,Links,Labels,Children bool}
type LogicalContext struct{Value string}
` + "type Record struct{Id *int64 `sqlx:\"id,primaryKey\"`;TenantId int64 `sqlx:\"tenant_id,primaryKey\"`;ParentId *int64 `sqlx:\"parent_id\" validate:\"gt(50)\"`;Name string `sqlx:\"name\"`;Context string `sqlx:\"-\"`;Details *LogicalContext `sqlx:\"-\"`;Links []*LogicalContext `sqlx:\"-\"`;Labels []string `sqlx:\"-\"`;Children []*Record `sqlx:\"-\"`;Has *Marker `sqlx:\"-\" setMarker:\"true\"`}\n" +
	"type Previous struct{Id *int64 `sqlx:\"id\"`;TenantId int64 `sqlx:\"tenant_id\"`;ParentId *int64 `sqlx:\"parent_id\"`;Name string `sqlx:\"name\"`}\n" + `
type Input struct{Events []*Record;CurrentEvents,CurrentChildren []*Previous}
type Output struct{Data []*Record}
var customCalls,queueCalls int
type Hooks struct{}
func(*Hooks)Init(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{return nil}
func(*Hooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{customCalls++;return nil}
func(*Hooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{queueCalls++;return nil}
type ChildHooks struct{}
func(*ChildHooks)Init(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{return nil}
func(*ChildHooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{customCalls++;return nil}
func(*ChildHooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{queueCalls++;return nil}
func TestReconciledCoverage(t *testing.T){
 ctx:=context.Background();h:=sqlite.New(t)
 if err:=h.ExecStatements(ctx,"CREATE TABLE records(tenant_id INTEGER NOT NULL,id INTEGER NOT NULL,parent_id INTEGER,name TEXT,PRIMARY KEY(tenant_id,id))","INSERT INTO records VALUES(5,10,99,'parent'),(7,20,99,'child')");err!=nil{t.Fatal(err)}
 bindings:=[]bindly.BindingSpec{{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}}}
 component:=&spec.Component{}
 for _,name:=range []string{"CurrentEvents","CurrentChildren"}{
  bindings=append(bindings,bindly.BindingSpec{Path:name,Name:name,Location:bindstate.Location{Kind:"view",In:name}})
  component.Parameters=append(component.Parameters,&spec.Parameter{Name:name,Source:spec.BindSource{Kind:"view",Name:name}})
  component.Views=append(component.Views,&spec.View{Name:name,Source:&spec.ViewSource{SQL:"SELECT id,tenant_id,parent_id,name FROM records"}})
 }
 seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
 bound,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=bound.Projection();if err!=nil{t.Fatal(err)}
 dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:inputType,Bindings:bindings});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:projection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
 ref:=spec.RouteRef{Method:"PATCH",Path:"/records"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:bound,Bindings:bindings});if err!=nil{t.Fatal(err)};route,ok:=contract.ForRoute(ref);if !ok{t.Fatal("route missing")}
 parentID,childID:=int64(10),int64(20)
 child:=&Record{Id:&childID,TenantId:7,Name:"changed child",Has:&Marker{Id:true,TenantId:true,Name:true}}
 parent:=&Record{Id:&parentID,TenantId:5,Name:"changed parent",Has:&Marker{Id:true,TenantId:true,Name:true},Children:[]*Record{child}}
 definition:={{FACTORY}}().(*{{DEFINITION}})
 _,err=engine.New().Execute(ctx,engine.Request{Input:route,Handler:mutation.New[Input,Output](definition),DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{views,values.New("test",map[string]any{"events":[]*Record{parent}})}})
 var failed *handler.Validation
 if !errors.As(err,&failed)||len(failed.Violations)!=1{t.Fatalf("expected final link-rule failure: %v",err)}
 v:=failed.Violations[0];if v.Field!="ParentId"||v.Location!="Input.Events.Children[0].ParentId"{t.Fatalf("wrong violation: %+v",v)}
 if customCalls!=2||queueCalls!=0{t.Fatalf("wrong gate: custom=%d queued=%d error=%v",customCalls,queueCalls,err)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name,parent_id FROM records ORDER BY id"},[]struct{Name string;ParentId int64}{{"parent",99},{"child",99}})
}
`

const sequenceValidationSQLiteFixture = `package events
import (
 "context";"errors";"fmt";"reflect";"sort";"strings";"testing";"time"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/handler/mutation";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";"github.com/viant/datly/sql/dml"
 "github.com/viant/xdatly/handler"
 "github.com/viant/govalidator"
 policy "github.com/viant/xdatly/handler/mutation"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
type Marker struct{Id,TenantId,ParentId,Name,Context,Details,Links,Labels,Children bool}
type LogicalContext struct{Value string}
type serviceKey struct{}
` + "type Record struct{Id *int64 `sqlx:\"id,primaryKey,unique,table=records\" validate:\"required,gt(-1)\"`;TenantId int64 `sqlx:\"tenant_id,primaryKey\"`;ParentId *int64 `sqlx:\"parent_id\"`;Name string `sqlx:\"name\" validate:\"required,sequence_probe\"`;Context string `sqlx:\"-\" validate:\"required,sequence_context\"`;Details *LogicalContext `sqlx:\"-\"`;Links []*LogicalContext `sqlx:\"-\"`;Labels []string `sqlx:\"-\" validate:\"required\"`;Children []*Record `sqlx:\"-\"`;Has *Marker `sqlx:\"-\" setMarker:\"true\"`;Service context.Context `sqlx:\"-\"`;Signal chan int `sqlx:\"-\"`;Callback func() int `sqlx:\"-\"`}\n" + `
type Input struct{Events []*Record;Mode string}
type Output struct{Data []*Record}
type observedSequenceDefinition struct{*{{DEFINITION}}}
func(d *observedSequenceDefinition)Capture(ctx context.Context,input *Input)(policy.Program[Output],error){p,err:=d.{{DEFINITION}}.Capture(ctx,input);if err==nil&&input.Mode=="produced collision"{return &badSequenceProgram{p.(*{{PROGRAM}})},nil};return p,err}
type badSequenceProgram struct{*{{PROGRAM}}}
func(p *badSequenceProgram)Prepare(ctx context.Context,binder handler.Binder)error{if err:=p.{{PROGRAM}}.Prepare(ctx,binder);err!=nil{return err};p.actions.Sequencer=badSequencer{};return nil}
type badSequencer struct{}
func(badSequencer)Allocate(_ context.Context,_ string,dest any,_ string)error{for _,row:=range dest.([]*Record){id:=int64(11);row.Id=&id};return nil}
var active *Input
var produced bool
var customCalls,afterSequenceCalls,queueCalls,finalCalls int
var cancelFinal context.CancelFunc
var outcomes []handler.Outcome
func init(){
 govalidator.RegisterWithDependencies("sequence_context",func(*govalidator.Field,*govalidator.Check)(govalidator.IsValid,error){
  return func(ctx context.Context,_ any)(bool,error){
   session:=ctx.Value(govalidator.SessionKey).(*govalidator.Session)
   row,ok:=session.ParentValue.(Record);if !ok{return false,fmt.Errorf("unexpected native context %T",session.ParentValue)}
   return row.Details!=nil&&row.Details.Value!=""&&len(row.Links)>0&&row.Links[0].Value!=""&&(row.TenantId!=0||len(row.Children)>0),nil
  },nil
 },func(*govalidator.Field,*govalidator.Check)([]string,error){return []string{"TenantId","Children","Details","Links"},nil})
 govalidator.RegisterWithDependencies("sequence_probe",func(*govalidator.Field,*govalidator.Check)(govalidator.IsValid,error){
  return func(ctx context.Context,value any)(bool,error){
   if active==nil{return true,nil}
   mode:=active.Mode
   if produced{finalCalls++}
   if (!produced&&mode=="native business value")||(produced&&mode=="native final value"){active.Events[0].Name="mutated"}
   if (!produced&&mode=="native business marker")||(produced&&mode=="native final marker"){active.Events[0].Has.Name=false}
   if produced&&mode=="native final logical marker"{active.Events[0].Has.Context=false}
   if produced&&finalCalls==2{
    switch mode{
    case "native final logical value":active.Events[0].Context=""
    case "native final logical pointer":active.Events[0].Details.Value=""
    case "native final relationship value":active.Events[0].Links[0].Value=""
    case "native final relation slice":active.Events[0].Labels[0]="changed"
    }
   }
   if produced&&mode=="final cancellation"{cancelFinal()}
   return !(produced&&mode=="final failure"),nil
  },nil
 },func(*govalidator.Field,*govalidator.Check)([]string,error){return nil,nil})
}
func(i *Input)Init(context.Context)error{active=i;if i.Mode=="initialized valid key"{*i.Events[0].Id=5};if i.Mode=="pending changed key"{id:=int64(50);i.Events[0].Children[0].Id=&id};return nil}
` + "type Hooks struct{Input *Input `bind:\"kind=input\"`}\n" + `
func(h *Hooks)Init(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{return nil}
func(h *Hooks)Validate(_ context.Context,row *Record,_ handler.LifecycleContext[Record,handler.NoParent,Output])error{
 customCalls++
 if h.Input.Mode=="custom value"{row.Name="mutated"}
 if h.Input.Mode=="custom marker"{row.Has.Name=false}
 if h.Input.Mode=="custom logical marker"{row.Has.Context=false}
 if h.Input.Mode=="custom logical value"{row.Context=""}
 if h.Input.Mode=="custom logical pointer"{row.Details.Value=""}
 if h.Input.Mode=="custom relationship value"{row.Links[0].Value=""}
 return nil
}
func(*Hooks)AfterSequence(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{produced=true;afterSequenceCalls++;return nil}
func(*Hooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{queueCalls++;return nil}
type ChildHooks struct{}
func(*ChildHooks)Init(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{return nil}
func(*ChildHooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{customCalls++;return nil}
func(*ChildHooks)AfterSequence(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{produced=true;afterSequenceCalls++;return nil}
func(*ChildHooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,Record,Output])error{queueCalls++;return nil}
type completion struct{}
func(*completion)Finalize(_ context.Context,_ *Input,_ *Output,outcome handler.Outcome)error{outcomes=append(outcomes,outcome.Clone());return nil}
func TestSequenceValidation(t *testing.T){
 for _,mode:=range []string{"success","pending supplied","pending changed key","unrelated tenant","explicit zero","supplied nil","ordinary missing","logical missing","children missing","relation missing","initialized valid key","custom value","custom marker","custom logical marker","custom logical value","custom logical pointer","custom relationship value","native business value","native business marker","native final value","native final marker","native final logical marker","native final logical value","native final logical pointer","native final relationship value","native final relation slice","produced collision","final failure","final cancellation"}{t.Run(mode,func(t *testing.T){
  active=nil;produced=false;customCalls=0;afterSequenceCalls=0;queueCalls=0;finalCalls=0;outcomes=nil
  ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel();cancelFinal=cancel
  h:=sqlite.New(t);h.DB.SetMaxOpenConns(1);h.DB.SetMaxIdleConns(1)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(tenant_id INTEGER NOT NULL,id INTEGER NOT NULL UNIQUE,parent_id INTEGER,name TEXT,PRIMARY KEY(tenant_id,id))","INSERT INTO records VALUES(5,10,NULL,'stored')");err!=nil{t.Fatal(err)}
  businessWrites:=h.ObserveWrites(t,ctx,"records")
  bindings:=[]bindly.BindingSpec{{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}},{Path:"Mode",Name:"Mode",Location:bindstate.Location{Kind:"test",In:"mode"}}}
  seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
  bound,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=bound.Projection();if err!=nil{t.Fatal(err)}
  ref:=spec.RouteRef{Method:"POST",Path:"/records"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:bound,Bindings:bindings});if err!=nil{t.Fatal(err)};route,ok:=contract.ForRoute(ref);if !ok{t.Fatal("route missing")}
  peer:=int64(20)
  first:=&Record{TenantId:0,Name:"first",Context:"logical context",Labels:[]string{"tag"},Has:&Marker{TenantId:true,Name:true,Context:true,Labels:true}}
  second:=&Record{TenantId:7,Id:&peer,Name:"second",Context:"other context",Labels:[]string{"tag"},Has:&Marker{Id:true,TenantId:true,Name:true,Context:true,Labels:true}}
  first.Children=[]*Record{second}
  details:=&LogicalContext{Value:"logical pointer"};link:=&LogicalContext{Value:"logical relationship"}
  service:=context.WithValue(ctx,serviceKey{},"opaque context");signal:=make(chan int)
  for _,row:=range []*Record{first,second}{row.Details=details;row.Links=[]*LogicalContext{link};row.Service=service;row.Signal=signal;row.Callback=func()int{return 42}}
  firstMarker,secondMarker:=first.Has,second.Has
  if mode=="native final logical pointer"{second.Details=&LogicalContext{Value:"other pointer"}}
  if mode=="native final relationship value"{second.Links=[]*LogicalContext{{Value:"other relationship"}}}
  switch mode{
  case "explicit zero":zero:=int64(0);first.Id=&zero;first.Has.Id=true
  case "supplied nil":first.Has.Id=true
  case "ordinary missing":first.Name=""
  case "logical missing":first.Context=""
  case "relation missing":first.Labels=nil
  case "initialized valid key":bad:=int64(-1);first.Id=&bad;first.Has.Id=true
  case "pending supplied","pending changed key","produced collision":peer=11
  case "unrelated tenant":second.TenantId=11
  }
  events:={{EVENTS}}
  if mode=="children missing"{first.Children=nil}
  definition:={{FACTORY}}().(*{{DEFINITION}});definition.Finalizer=&completion{}
  _,err=engine.New().Execute(ctx,engine.Request{Input:route,Handler:mutation.New[Input,Output](&observedSequenceDefinition{definition}),DataSource:dml.Source{DB:h.DB},Providers:[]locator.Provider{values.New("test",map[string]any{"events":events,"mode":mode})}})
  success:=mode=="success"||mode=="explicit zero"||mode=="pending supplied"||mode=="pending changed key"||mode=="unrelated tenant"||mode=="initialized valid key"
  if mode=="pending supplied"&&(first.Id==nil||*first.Id!=12||*second.Id!=11){t.Fatalf("pending supplied identity was not reserved: %+v / %+v",first,second)}
  if mode=="pending changed key"&&(first.Id==nil||*first.Id!=11||*second.Id!=50){t.Fatalf("effective initialized key not reserved: %+v / %+v",first,second)}
  if mode=="unrelated tenant"&&(first.Id==nil||*first.Id!=11){t.Fatal("allocator reserved an unrelated composite key part")}
  if (err==nil)!=success{var id int64;if first.Id!=nil{id=*first.Id};t.Fatalf("execution: %v (first ID=%d)",err,id)}
  if len(outcomes)!=1||outcomes[0].CommitConfirmed()!=success||(outcomes[0].Error==nil)!=success{t.Fatalf("outcomes=%+v error=%v",outcomes,err)}
  beforeHooks:=mode=="supplied nil"||mode=="ordinary missing"||mode=="logical missing"||mode=="relation missing"||mode=="children missing"||strings.HasPrefix(mode,"native business")
  if beforeHooks&&(customCalls!=0||afterSequenceCalls!=0){t.Fatalf("business failure reached hooks: %v counts=%d/%d",err,customCalls,afterSequenceCalls)}
  if strings.HasPrefix(mode,"custom ")&&afterSequenceCalls!=0{t.Fatal("custom mutation reached allocation")}
  if beforeHooks&&mode!="supplied nil"&&first.Id!=nil{t.Fatal("business failure allocated ID")}
  if mode=="ordinary missing"||mode=="logical missing"||mode=="relation missing"||mode=="children missing"{
   field:=map[string]string{"ordinary missing":"Name","logical missing":"Context","relation missing":"Labels","children missing":"Context"}[mode]
   check:="required";if mode=="children missing"{check="sequence_context"}
   var failed *handler.Validation;if !errors.As(err,&failed){t.Fatalf("expected native required %s failure: %v",field,err)}
   found:=false;for _,v:=range failed.Violations{found=found||(v.Field==field&&v.Check==check)};if !found{t.Fatalf("required %s did not run: %+v",field,failed)}
  }
  if !beforeHooks&&!strings.HasPrefix(mode,"custom ")&&(customCalls!=2||afterSequenceCalls!=2){t.Fatalf("expected one custom pass before final: %v counts=%d/%d",err,customCalls,afterSequenceCalls)}
  if !success&&queueCalls!=0{t.Fatal("failed final check reached Queue")}
  if strings.Contains(mode,"value")||strings.Contains(mode,"marker"){if !strings.Contains(err.Error(),"written field or marker"){t.Fatalf("mutation guard error=%v",err)}}
  if mode=="native final logical pointer"||mode=="native final relation slice"{if !strings.Contains(err.Error(),"validation context"){t.Fatalf("logical mutation guard error=%v",err)}}
  if mode=="produced collision"{
   var failed *handler.Validation;if !errors.As(err,&failed)||len(failed.Violations)!=2{t.Fatalf("expected two final UNIQUE violations: %v",err)}
   locations:=[]string{};for _,v:=range failed.Violations{if v.Check!="unique"||v.Field!="Id"{t.Fatalf("wrong violation %+v",v)};locations=append(locations,v.Location)}
   expected:=[]string{"Input.Events[0].Id","{{SECOND_LOCATION}}"};sort.Strings(locations);sort.Strings(expected);if !reflect.DeepEqual(locations,expected){t.Fatalf("locations=%v want=%v",locations,expected)}
  }
  if mode=="final cancellation"&&!errors.Is(err,context.Canceled){t.Fatalf("cancellation lost: %v",err)}
  if mode=="initialized valid key"{if *first.Id!=5{t.Fatal("initialized key not retained")};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records WHERE id=5"},[]struct{Name string}{{"first"}})}
  queryCtx:=context.Background();want:=1;if success{want=3;if queueCalls!=2||finalCalls!=2{t.Fatalf("final context checks/queue=%d/%d",finalCalls,queueCalls)};if first.Context==""||len(first.Labels)==0{t.Fatal("logical context dropped")};if mode=="explicit zero"&&*first.Id!=0{t.Fatal("supplied zero allocated")}}
  h.AssertQuery(t,queryCtx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM records"},[]struct{N int}{{want}})
  if success&&(first.Details!=details||second.Details!=details||first.Links[0]!=link||second.Links[0]!=link||first.Has!=firstMarker||second.Has!=secondMarker||first.Service!=service||first.Signal!=signal||first.Callback()==0){t.Fatal("context aliases, holders or opaque services were changed")}
  // Observe business rows directly: durable native reservations also write
  // metadata, and must not be confused with early entity writes.
  if got:=businessWrites.Load();got!=int64(want-1){t.Fatalf("business writes=%d want=%d",got,want-1)}
 })}
}
type noopSequencer struct{calls int}
func(s *noopSequencer)Allocate(context.Context,string,any,string)error{s.calls++;return nil}
type programBinder struct{input *Input;data *dml.Data;sequence *noopSequencer}
func(b programBinder)Bind(_ context.Context,value any)error{
 switch target:=value.(type){
 case *Hooks:target.Input=b.input
 case *ChildHooks:
 case *_newEventsHandlerMutationActions:target.Input=b.input;target.DML=b.data;target.Sequencer=b.sequence
 case *_newEventsHandlerFrameworkValidation:target.Validator=b.data.FrameworkValidator()
 default:return fmt.Errorf("unexpected binding %T",value)
 };return nil
}
func(b programBinder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,nil}
func TestNoopSequenceCannotQueue(t *testing.T){
 active=nil;produced=false;outcomes=nil;ctx:=context.Background();h:=sqlite.New(t)
 if err:=h.ExecStatements(ctx,"CREATE TABLE records(tenant_id INTEGER NOT NULL,id INTEGER NOT NULL UNIQUE,parent_id INTEGER,name TEXT,PRIMARY KEY(tenant_id,id))");err!=nil{t.Fatal(err)}
 childID:=int64(1)
 child:=&Record{Id:&childID,TenantId:7,Name:"child",Context:"context",Labels:[]string{"tag"},Has:&Marker{Id:true,TenantId:true,Name:true,Context:true,Labels:true}}
 input:=&Input{Events:[]*Record{{Name:"new",Context:"context",Labels:[]string{"tag"},Children:[]*Record{child},Has:&Marker{TenantId:true,Name:true,Context:true,Labels:true}}}}
 for _,row:=range []*Record{input.Events[0],child}{row.Details=&LogicalContext{Value:"context"};row.Links=[]*LogicalContext{{Value:"link"}}}
 definition:={{FACTORY}}().(*{{DEFINITION}});definition.Finalizer=&completion{}
 program,err:=definition.Capture(ctx,input);if err!=nil{t.Fatal(err)}
 data:=dml.NewData(h.DB);if err=data.BeginInvocation();err!=nil{t.Fatal(err)};defer data.Complete(ctx,fmt.Errorf("cleanup"))
 seq:=&noopSequencer{};if err=program.Prepare(ctx,programBinder{input,data,seq});err!=nil{t.Fatal(err)}
 for _,phase:=range []func(context.Context)error{program.SyncPresence,program.Invariants,program.Init,program.Validate}{if err=phase(ctx);err!=nil{t.Fatal(err)}}
 if err=data.Start(ctx);err!=nil{t.Fatal(err)};if err=program.Sequence(ctx);err!=nil{t.Fatal(err)}
 if seq.calls!=1{t.Fatalf("allocator calls=%d",seq.calls)}
 if err=program.(interface{AfterSequence(context.Context)error}).AfterSequence(ctx);err!=nil{t.Fatal(err)}
 failure:=program.Diff(ctx);if failure==nil{t.Fatal("noop producer passed readiness")}
 if err=program.Queue(ctx);err==nil{t.Fatal("failed readiness allowed Queue")}
 if err=data.Complete(ctx,failure);err==nil{t.Fatal("expected failed completion")}
 if err=program.Finalize(ctx,handler.Outcome{Error:failure});err!=nil{t.Fatal(err)}
 if len(outcomes)!=1||outcomes[0].CommitConfirmed(){t.Fatalf("outcomes=%+v",outcomes)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM records"},[]struct{N int}{{0}})
}
`
