package golang

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

type relationProducerFixture struct {
	self      bool
	mode      string
	composite bool
	velty     bool
	callerTx  bool
}

func (f relationProducerFixture) program(t *testing.T) *MutationProgramAsset {
	t.Helper()
	semantic := rootSemanticPlan(plan.OperationPost, false)
	if strings.HasPrefix(f.mode, "parent update") || f.mode == "child update" || f.mode == "partial current" {
		semantic.Operation = plan.OperationPatch
		semantic.Root.Write = fixtureWritePolicy(plan.OperationPatch, semantic.Root.InputPath, 0)
	}
	root := semantic.Root
	root.Table = "nodes"
	root.Keys[0].Source = "id"
	root.Sequence.Field = plan.FieldRef{Field: "Id", Source: "id", Type: spec.TypeRef{Name: "*int64"}}
	root.Entity = &plan.EntityPlan{
		Owned: true, Type: spec.TypeRef{Name: "Record"}, Keys: cloneFixtureKeys(root.Keys),
		MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"},
		Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true,
		Fields: []plan.EntityField{
			{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
			{Name: "ParentId", Type: spec.TypeRef{Name: "*int64"}, Writable: true},
			{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
			{Name: "Children", Type: spec.TypeRef{Name: "[]*Record"}, Relation: true, Self: f.self, Writable: true},
		},
	}
	links := []plan.KeyLink{{Parent: root.Keys[0], Child: plan.KeyPart{Field: "ParentId", Source: "parent_id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}}
	if f.mode == "partial key" || f.composite {
		root.Keys = append(root.Keys, links[0].Child)
		root.Entity.Keys = cloneFixtureKeys(root.Keys)
		root.Entity.Fields[1].Identity = true
	}
	if f.composite {
		root.Entity.Fields = append(root.Entity.Fields, plan.EntityField{Name: "AncestorId", Type: spec.TypeRef{Name: "*int64"}, Writable: true})
		links = append(links, plan.KeyLink{Parent: links[0].Child, Child: plan.KeyPart{Field: "AncestorId", Source: "ancestor_id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect})
	}
	types := rootRecordTypes(semantic, "[]*Record", "")
	if f.self {
		root.SelfRelations = []plan.SelfRelationPlan{{FieldPath: plan.FieldPath{"Children"}, Links: links}}
		if f.mode == "competing producers" {
			root.Sequence.Field = plan.FieldRef{Field: "ParentId", Source: "parent_id", Type: spec.TypeRef{Name: "*int64"}}
		}
	} else {
		entity := *root.Entity
		entity.Hooks = spec.TypeRef{Name: "ChildHooks"}
		entity.Fields = append([]plan.EntityField(nil), root.Entity.Fields[:3]...)
		if f.composite {
			entity.Fields = append(entity.Fields, root.Entity.Fields[len(root.Entity.Fields)-1])
		}
		child := &plan.RecordPlan{Identity: "view::Children|namespace:", InputPath: plan.FieldPath{"Input", "Events", "Children"}, Table: root.Table, Cardinality: spec.CardinalityMany, Keys: cloneFixtureKeys(root.Keys), Entity: &entity, Write: fixtureWritePolicy(plan.OperationPost, plan.FieldPath{"Input", "Events", "Children"}, 1)}
		if f.mode == "pending alias" {
			child.Table = `"MAIN"."NODES"`
		}
		if f.mode == "two allocations" {
			child.Sequence = &plan.SequencePlan{Destination: child.InputPath, Selector: plan.FieldPath{"Id"}, Field: root.Sequence.Field}
		}
		if f.mode == "competing producers" {
			child.Sequence = &plan.SequencePlan{Destination: child.InputPath, Selector: plan.FieldPath{"ParentId"}, Field: plan.FieldRef{Field: "ParentId", Source: "parent_id", Type: spec.TypeRef{Name: "*int64"}}}
		}
		if strings.HasPrefix(f.mode, "parent update") || f.mode == "child update" || f.mode == "partial current" {
			child.Write = fixtureWritePolicy(plan.OperationPatch, child.InputPath, 1)
		}
		root.Relations = []*plan.RelationPlan{{Identity: "Children", FieldPath: plan.FieldPath{"Children"}, Cardinality: spec.CardinalityMany, Links: links, Child: child}}
		types = append(types, RecordType{Identity: child.Identity, Path: child.InputPath, Value: "[]*Record"})
	}
	if f.mode == "incomplete key" {
		records := []*plan.RecordPlan{root}
		if !f.self {
			records = append(records, root.Relations[0].Child)
		}
		for _, record := range records {
			record.Keys = append(record.Keys, plan.KeyPart{Field: "Name", Source: "name", Type: spec.TypeRef{Name: "string"}})
			record.Entity.Keys = cloneFixtureKeys(record.Keys)
		}
	}
	if strings.HasPrefix(f.mode, "parent update") || f.mode == "child update" || f.mode == "partial current" {
		records := []*plan.RecordPlan{root}
		if !f.self {
			records = append(records, root.Relations[0].Child)
		}
		for index, record := range records {
			name := "CurrentEvents"
			if index > 0 {
				name = "CurrentChildren"
			}
			record.Current = &plan.CurrentPlan{ParamIdentity: name, ViewIdentity: "view::" + name + "|namespace:", InputPath: plan.FieldPath{"Input", name}, Keys: cloneFixtureKeys(record.Keys)}
			for _, field := range record.Entity.Fields {
				if !field.Relation {
					record.Current.Fields = append(record.Current.Fields, plan.CurrentField{Current: plan.FieldRef{Field: field.Name, Type: field.Type}, Entity: plan.FieldRef{Field: field.Name, Type: field.Type}, Conversion: plan.LinkDirect})
				}
			}
			types[index].Current = "[]*Record"
		}
	}
	asset, err := MutationProgram(semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
	if err != nil {
		t.Fatal(err)
	}
	return asset
}

// Exercise the complete generated policy through canonical Data and native SQLite checks.
func TestRelationProducerProgramSQLite(t *testing.T) {
	for _, self := range []bool{false, true} {
		layout := "relation"
		if self {
			layout = "self"
		}
		t.Run(layout, func(t *testing.T) {
			for _, mode := range []string{"absent child", "produced Go", "produced Go failure", "working marker", "supplied nil changed", "supplied zero changed", "incomplete key", "reference boundary", "partial key", "parent update", "parent update changed", "parent update supplied", "child update", "supplied nil", "supplied zero", "supplied conflict", "unrelated root", "ordinary Go rule", "produced unique", "pending identity", "pending alias", "two allocations", "bad allocator", "altered topology", "ambiguous graph", "competing producers", "reversed order", "wrong target"} {
				t.Run(mode, func(t *testing.T) {
					(relationProducerFixture{self: self, mode: mode}).run(t)
				})
			}
		})
	}
}

func (f relationProducerFixture) run(t *testing.T) {
	t.Helper()
	self, mode := f.self, f.mode
	asset := f.program(t)
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	childLocation, otherLocation, uniqueLocations := "Input.Events.Children[0].ParentId", "Input.Events[1].ParentId", `[]string{"Input.Events.Children[0].Name","Input.Events.Children[1].Name"}`
	selfLiteral := "false"
	if self {
		childLocation, otherLocation, uniqueLocations = "Input.Events[1].ParentId", "Input.Events[2].ParentId", `[]string{"Input.Events[1].Name","Input.Events[2].Name"}`
		selfLiteral = "true"
	}
	source := strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition, "{{PROGRAM}}", asset.Program, "{{ORDER}}", asset.Frames.Layout.OrderField, "{{MODE}}", mode, "{{SELF}}", selfLiteral, "{{CHILD_LOCATION}}", childLocation, "{{OTHER_LOCATION}}", otherLocation, "{{UNIQUE_LOCATIONS}}", uniqueLocations).Replace(relationProducerSQLiteSource)
	if mode == "wrong target" {
		source = strings.NewReplacer("refTable=nodes", "refTable=othernodes", "REFERENCES nodes(id)", "REFERENCES othernodes(id)").Replace(source)
	}
	source = f.refineSource(source)
	(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
}

const relationProducerSQLiteSource = `package events
import (
 "context";"errors";"reflect";"sort";"strings";"sync/atomic";"testing";"time"
 "github.com/viant/bindly";"github.com/viant/bindly/locator";"github.com/viant/bindly/provider/values"
 bindstate "github.com/viant/bindly/state"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/runtime/handler/mutation";"github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec";dsql "github.com/viant/datly/sql";"github.com/viant/datly/sql/dml";"github.com/viant/datly/sql/reader/compiler";viewprovider "github.com/viant/datly/sql/reader/provider"
 "github.com/viant/xdatly/handler"
 policy "github.com/viant/xdatly/handler/mutation"
 sqlite3 "github.com/mattn/go-sqlite3"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
const mode="{{MODE}}"
type Marker struct{Id,ParentId,Name,Children bool}
` + "type Record struct{Id *int64 `sqlx:\"id,primaryKey\" validate:\"required\"`;ParentId *int64 `sqlx:\"parent_id,required,refTable=nodes,refColumn=id\"`;Name string `sqlx:\"name,unique,uniqueDep=parent_id,table=nodes\" validate:\"required\"`;Children []*Record `sqlx:\"-\"`;Has *Marker `sqlx:\"-\" setMarker:\"true\"`}\n" + `
type Input struct{Events []*Record;CurrentEvents,CurrentChildren []*Record}
func(i *Input)Init(context.Context)error{if mode=="parent update changed"{id:=int64(99);i.Events[0].Id=&id;i.Events[0].Has.Id=false;for _,row:=range i.CurrentEvents{*row.Id=99}};if mode=="working marker"||mode=="supplied nil changed"||mode=="supplied zero changed"{i.Events[0].Children[0].ParentId=i.Events[0].ParentId;i.Events[0].Children[0].Has.ParentId=true};if mode=="altered topology"{i.Events[1].Children=i.Events[0].Children;i.Events[0].Children=nil};return nil}
type Output struct{Data []*Record}
var parent *Record
var initCalls,activeEdges,customCalls,sequenceCalls,queueCalls int
var writes atomic.Int64
var outcomes []handler.Outcome
var captured *{{PROGRAM}}
type observedDefinition struct{*{{DEFINITION}}}
func(d *observedDefinition)Capture(ctx context.Context,input *Input)(policy.Program[Output],error){p,err:=d.{{DEFINITION}}.Capture(ctx,input);if err==nil{captured=p.(*{{PROGRAM}});return &observedProgram{captured},nil};return p,err}
type observedProgram struct{*{{PROGRAM}}}
func(p *observedProgram)Prepare(ctx context.Context,binder handler.Binder)error{if err:=p.{{PROGRAM}}.Prepare(ctx,binder);err!=nil{return err};if mode=="bad allocator"{p.actions.Sequencer=badSequencer{}};return nil}
func(p *observedProgram)Queue(ctx context.Context)error{if strings.HasPrefix(mode,"pending ")||mode=="two allocations"{childID:=int64(6);if mode=="two allocations"{childID=8};if parent.Id==nil||*parent.Id!=7||parent.Children[0].Id==nil||*parent.Children[0].Id!=childID||parent.Children[0].ParentId==nil||*parent.Children[0].ParentId!=7{return errors.New("IDs or links not final before Queue")};if mode=="two allocations"&&(*parent.Children[1].Id!=6||parent.Children[1].ParentId==nil||*parent.Children[1].ParentId!=7){return errors.New("supplied sibling changed before Queue")}};return p.{{PROGRAM}}.Queue(ctx)}
type badSequencer struct{}
func(badSequencer)Allocate(_ context.Context,_ string,dest any,_ string)error{for _,row:=range dest.([]*Record){id:=int64(6);row.Id=&id};return nil}
type Hooks struct{}
func(*Hooks)Init(_ context.Context,row *Record,state handler.EntityState[Record,handler.NoParent])error{
 initCalls++
 if row!=parent&&row.Name!="unrelated"{
  if !{{SELF}}||state.SelfParent!=parent{return errors.New("self edge did not reach typed frame")};activeEdges++
 }else if state.SelfParent!=nil{return errors.New("unrelated root acquired a self producer")}
 if {{SELF}}&&state.SelfParent!=nil&&mode=="reference boundary"{row.ParentId=parent.ParentId}
 return nil
}
func(*Hooks)Validate(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{customCalls++;return noEarlyWrite()}
func(*Hooks)AfterSequence(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{sequenceCalls++;if mode=="reversed order"&&sequenceCalls==1{order:=captured.frames.{{ORDER}};order[0],order[len(order)-1]=order[len(order)-1],order[0]};return noEarlyWrite()}
func(*Hooks)AfterQueue(context.Context,*Record,handler.EntityState[Record,handler.NoParent])error{queueCalls++;return noEarlyWrite()}
type ChildHooks struct{}
func(*ChildHooks)Init(_ context.Context,row *Record,state handler.EntityState[Record,Record])error{
 initCalls++;if state.Parent!=parent||state.SelfParent!=nil{return errors.New("canonical relation parent missing")};activeEdges++;if mode=="reference boundary"{row.ParentId=parent.ParentId};return nil
}
func(*ChildHooks)Validate(context.Context,*Record,handler.EntityState[Record,Record])error{customCalls++;return noEarlyWrite()}
func(*ChildHooks)AfterSequence(context.Context,*Record,handler.EntityState[Record,Record])error{sequenceCalls++;return noEarlyWrite()}
func(*ChildHooks)AfterQueue(context.Context,*Record,handler.EntityState[Record,Record])error{queueCalls++;return noEarlyWrite()}
func noEarlyWrite()error{if writes.Load()!=0{return errors.New("entity SQL executed before buffered completion")};return nil}
type completion struct{}
func(*completion)Finalize(_ context.Context,_ *Input,_ *Output,outcome handler.Outcome)error{outcomes=append(outcomes,outcome.Clone());return nil}
func TestPendingGraph(t *testing.T){
 ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel()
 h:=sqlite.New(t);h.DB.SetMaxOpenConns(1);h.DB.SetMaxIdleConns(1)
 if err:=h.ExecStatements(ctx,"PRAGMA foreign_keys=ON","CREATE TABLE othernodes(id INTEGER PRIMARY KEY)","INSERT INTO othernodes VALUES(5)","CREATE TABLE nodes(id INTEGER PRIMARY KEY,parent_id INTEGER NOT NULL,name TEXT NOT NULL,UNIQUE(parent_id,name),FOREIGN KEY(parent_id) REFERENCES nodes(id))","INSERT INTO nodes VALUES(5,5,'anchor')");err!=nil{t.Fatal(err)}
 if mode=="child update"{if err:=h.ExecStatements(ctx,"INSERT INTO nodes VALUES(20,5,'old child')");err!=nil{t.Fatal(err)}}
 connection,err:=h.DB.Conn(ctx);if err!=nil{t.Fatal(err)}
 err=connection.Raw(func(raw any)error{raw.(*sqlite3.SQLiteConn).RegisterUpdateHook(func(_ int,_ string,table string,_ int64){if table=="nodes"{writes.Add(1)}});return nil})
 if closeErr:=connection.Close();err!=nil{t.Fatal(err)}else if closeErr!=nil{t.Fatal(closeErr)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"PRAGMA foreign_keys"},[]struct{ForeignKeys int}{{1}})
 anchor,childID:=int64(5),int64(20)
 if strings.HasPrefix(mode,"pending ")||mode=="bad allocator"{childID=6}
 parent=&Record{ParentId:&anchor,Name:"parent",Has:&Marker{ParentId:true,Name:true}}
 child:=&Record{Id:&childID,Name:"child",Has:&Marker{Id:true,Name:true}}
 parent.Children=[]*Record{child};events:=[]*Record{parent}
 wantRows,wantEdges:=2,1
 switch mode{
 case "incomplete key":child.Has.Name=false
 case "two allocations":child.Id=nil;child.Has.Id=false;id:=int64(6);parent.Children=append(parent.Children,&Record{Id:&id,Name:"provided",Has:&Marker{Id:true,Name:true}});wantRows=3;wantEdges=2
 case "parent update","parent update changed","parent update supplied":parent.Id=&anchor;parent.Has.Id=true;if mode=="parent update supplied"{child.ParentId=&anchor;child.Has.ParentId=true}
 case "competing producers":if {{SELF}}{id:=int64(30);parent.Id=&id;parent.Has.Id=true}
 case "supplied conflict":child.ParentId=&anchor;child.Has.ParentId=true
 case "supplied nil","supplied nil changed":child.Has.ParentId=true
 case "supplied zero","supplied zero changed":zero:=int64(0);child.ParentId=&zero;child.Has.ParentId=true
 case "unrelated root":child.ParentId=&anchor;child.Has.ParentId=true;id:=int64(30);events=append(events,&Record{Id:&id,Name:"unrelated",Has:&Marker{Id:true,Name:true}});wantRows=3
 case "ordinary Go rule":child.ParentId=&anchor;child.Has.ParentId=true;child.Name=""
 case "produced unique":id:=int64(21);parent.Children=append(parent.Children,&Record{Id:&id,Name:"child",Has:&Marker{Id:true,Name:true}});wantRows=3;wantEdges=2
 case "altered topology":id:=int64(30);events=append(events,&Record{Id:&id,ParentId:&anchor,Name:"other",Has:&Marker{Id:true,ParentId:true,Name:true}})
 case "ambiguous graph":parent.Children=append(parent.Children,child)
 }
 bindings:=[]bindly.BindingSpec{{Path:"Events",Name:"Events",Location:bindstate.Location{Kind:"test",In:"events"}}}
 component:=&spec.Component{}
 if strings.HasPrefix(mode,"parent update")||mode=="child update"||mode=="partial current"{for _,name:=range []string{"CurrentEvents","CurrentChildren"}{
  bindings=append(bindings,bindly.BindingSpec{Path:name,Name:name,Location:bindstate.Location{Kind:"view",In:name}})
  component.Parameters=append(component.Parameters,&spec.Parameter{Name:name,Source:spec.BindSource{Kind:"view",Name:name}})
  component.Views=append(component.Views,&spec.View{Name:name,Source:&spec.ViewSource{SQL:"SELECT id,parent_id,name FROM nodes"}})
 }}
 seed,err:=bindly.NewInjector();if err!=nil{t.Fatal(err)};inputType:=reflect.TypeOf(Input{})
 bound,err:=seed.CompilePlan(inputType,bindings...);if err!=nil{t.Fatal(err)};projection,err:=bound.Projection();if err!=nil{t.Fatal(err)}
 providers:=[]locator.Provider{values.New("test",map[string]any{"events":events})}
 method:="POST"
 if strings.HasPrefix(mode,"parent update")||mode=="child update"||mode=="partial current"{
  method="PATCH";dependencies,err:=compiler.CompileViewDependencies(compiler.Input{Component:component,InputType:inputType,Bindings:bindings});if err!=nil{t.Fatal(err)}
  views,err:=viewprovider.New(viewprovider.Config{Dependencies:dependencies,Input:projection,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)};providers=append(providers,views)
 }
 ref:=spec.RouteRef{Method:method,Path:"/nodes"};contract,err:=registry.NewInputContract(inputType,projection,registry.RouteInput{Route:ref,Plan:bound,Bindings:bindings});if err!=nil{t.Fatal(err)};route,ok:=contract.ForRoute(ref);if !ok{t.Fatal("route missing")}
 definition:=&observedDefinition{ {{FACTORY}}().(*{{DEFINITION}}) };definition.Finalizer=&completion{}
 _,err=engine.New().Execute(ctx,engine.Request{Input:route,Handler:mutation.New[Input,Output](definition),DataSource:dml.Source{DB:h.DB},Providers:providers})
 if mode=="incomplete key"{if err==nil||!strings.Contains(err.Error(),"partial original identity")||customCalls!=0||sequenceCalls!=0||queueCalls!=0||writes.Load()!=0{t.Fatalf("incomplete captured key acquired producer authority: %v",err)};return}
 if mode=="produced Go failure"{var failed *handler.Validation;if !errors.As(err,&failed)||customCalls!=2||sequenceCalls!=2||queueCalls!=0||writes.Load()!=0||len(failed.Violations)!=1||failed.Violations[0].Check!="produced_fk"{t.Fatalf("final Go rule did not validate produced value: %v custom=%d sequence=%d",err,customCalls,sequenceCalls)};return}
 if mode=="parent update"||mode=="parent update changed"{var failed *handler.Validation;if err==nil||!errors.As(err,&failed)||len(failed.Violations)!=1||failed.Violations[0].Field!="ParentId"||failed.Violations[0].Check!="notnull"||customCalls!=0||sequenceCalls!=0||queueCalls!=0||writes.Load()!=0{t.Fatalf("parent UPDATE acquired INSERT producer deferral: err=%v custom=%d sequence=%d queue=%d writes=%d",err,customCalls,sequenceCalls,queueCalls,writes.Load())};return}
 if mode=="bad allocator"{if err==nil||!strings.Contains(err.Error(),"reconciled mutation identities are duplicated")||queueCalls!=0||writes.Load()!=0||len(outcomes)!=1||outcomes[0].CommitConfirmed(){t.Fatalf("cross-role identity collision reached Queue: err=%v queue=%d writes=%d",err,queueCalls,writes.Load())};return}
 if mode=="altered topology"||mode=="ambiguous graph"{
  if err==nil||initCalls!=0||customCalls!=0||queueCalls!=0||writes.Load()!=0||len(outcomes)!=1||outcomes[0].CommitConfirmed(){t.Fatalf("captured topology was not enforced: err=%v init=%d custom=%d writes=%d",err,initCalls,customCalls,writes.Load())}
  return
 }
 if mode=="competing producers"{if err==nil||!strings.Contains(err.Error(),"competing captured producers")||customCalls!=0||queueCalls!=0||writes.Load()!=0{t.Fatalf("competing producers accepted: %v",err)};return}
 if mode=="reversed order"{if err==nil||!strings.Contains(err.Error(),"execution order")||customCalls!=2||queueCalls!=0||writes.Load()!=0{t.Fatalf("reversed order escaped existing frame verifier: %v",err)};return}
 if initCalls!=wantRows||activeEdges!=wantEdges{t.Fatalf("typed topology missing: init=%d edges=%d error=%v",initCalls,activeEdges,err)}
 if len(outcomes)!=1{t.Fatalf("finalizers=%d error=%v",len(outcomes),err)}
 if (mode=="absent child"||mode=="produced Go"||mode=="working marker"||mode=="reference boundary"||mode=="partial key"||mode=="partial current"||mode=="parent update supplied"||strings.HasPrefix(mode,"pending ")||mode=="two allocations")&&err==nil{
  expectedID:=int64(6);if mode=="parent update supplied"{expectedID=5}
  if strings.HasPrefix(mode,"pending ")||mode=="two allocations"{expectedID=7}
  if parent.Id==nil||*parent.Id!=expectedID||child.ParentId==nil||*child.ParentId!=expectedID{t.Fatal("producer did not reconcile the exact parent")}
  if customCalls!=wantRows||sequenceCalls!=wantRows||queueCalls!=wantRows||writes.Load()!=int64(wantRows)||!outcomes[0].CommitConfirmed(){t.Fatalf("incorrect phase/write outcome custom=%d sequence=%d queue=%d writes=%d outcome=%+v",customCalls,sequenceCalls,queueCalls,writes.Load(),outcomes)}
  expected:=[]struct{Id,ParentId int64;Name string}{{5,5,"anchor"},{6,5,"parent"},{20,6,"child"}}
  if mode=="parent update supplied"{expected=[]struct{Id,ParentId int64;Name string}{{5,5,"parent"},{20,5,"child"}}}
  if strings.HasPrefix(mode,"pending "){expected=[]struct{Id,ParentId int64;Name string}{{5,5,"anchor"},{6,7,"child"},{7,5,"parent"}}}
  if mode=="two allocations"{expected=[]struct{Id,ParentId int64;Name string}{{5,5,"anchor"},{6,7,"provided"},{7,5,"parent"},{8,7,"child"}}}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,name FROM nodes ORDER BY id"},expected)
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"PRAGMA foreign_keys"},[]struct{ForeignKeys int}{{1}})
  if orphan:=h.ExecStatements(ctx,"INSERT INTO nodes VALUES(99,999,'orphan')");orphan==nil{t.Fatal("actual SQLite FK was disabled")}
  return
 }
 if mode=="supplied conflict"||mode=="supplied nil changed"||mode=="supplied zero changed"{
  if err==nil||!strings.Contains(err.Error(),"supplied relation field conflicts")||queueCalls!=0||writes.Load()!=0||*child.ParentId!=5{t.Fatalf("supplied link was overwritten: %v",err)}
  return
 }
 var failed *handler.Validation
 if !errors.As(err,&failed){t.Fatalf("expected real framework validation, got %v",err)}
 if queueCalls!=0||writes.Load()!=0||outcomes[0].CommitConfirmed(){t.Fatalf("failed validation wrote or queued: err=%v custom=%d queue=%d writes=%d outcome=%+v",err,customCalls,queueCalls,writes.Load(),outcomes)}
 if mode=="child update"{
  if customCalls!=2||sequenceCalls!=2||len(failed.Violations)!=1||failed.Violations[0].Check!="refKey"{t.Fatalf("UPDATE received producer privilege: %v",err)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,name FROM nodes ORDER BY id"},[]struct{Id,ParentId int64;Name string}{{5,5,"anchor"},{20,5,"old child"}})
  return
 }
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,name FROM nodes"},[]struct{Id,ParentId int64;Name string}{{5,5,"anchor"}})
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT total_changes() AS n"},[]struct{N int}{{2}})
 if mode=="wrong target"{if customCalls!=2||sequenceCalls!=2||len(failed.Violations)!=1||failed.Violations[0].Check!="refKey"{t.Fatalf("wrong target was certified: %v",err)};return}
 if mode=="reference boundary"{
  if customCalls!=2||sequenceCalls!=2||parent.Id==nil||*parent.Id!=6||len(failed.Violations)!=1{t.Fatalf("reference boundary not reached: %v custom=%d sequence=%d",err,customCalls,sequenceCalls)}
  v:=failed.Violations[0];if v.Check!="refKey"||v.Location!="{{CHILD_LOCATION}}"{t.Fatalf("wrong pending-reference violation: %+v",v)}
  t.Fatalf("pending-parent receipt failed: %v at %s; custom=2 sequence=2 queue=0 writes=0; no early flush",err,v.Location)
 }
 if mode=="produced unique"&&customCalls==3&&sequenceCalls==3{
  locations:=[]string{};for _,v:=range failed.Violations{if v.Check!="unique"||v.Field!="Name"{t.Fatalf("wrong final check: %+v",v)};locations=append(locations,v.Location)}
  expected:={{UNIQUE_LOCATIONS}};sort.Strings(locations);sort.Strings(expected);if !reflect.DeepEqual(locations,expected){t.Fatalf("unique cohort locations=%v want=%v",locations,expected)}
  return
 }
 if customCalls!=0||sequenceCalls!=0||parent.Id!=nil{t.Fatalf("unexpected failure phase: %v custom=%d sequence=%d",err,customCalls,sequenceCalls)}
 if mode=="absent child"||mode=="produced unique"{
  locations:=[]string{};for _,v:=range failed.Violations{if v.Check!="notnull"||v.Field!="ParentId"{t.Fatalf("unexpected business validation: %+v",v)};locations=append(locations,v.Location)}
  if mode=="absent child"&&(len(locations)!=1||locations[0]!="{{CHILD_LOCATION}}"){t.Fatalf("wrong active-child location: %v",locations)}
  t.Fatalf("active-edge deferral failed: mode=%s err=%v locations=%v init=%d activeEdges=%d custom=%d sequence=%d queue=%d writes=%d; want pending graph readiness/receipt path",mode,err,locations,initCalls,activeEdges,customCalls,sequenceCalls,queueCalls,writes.Load())
 }
 if len(failed.Violations)!=1{t.Fatalf("unexpected violations: %+v",failed)}
 v:=failed.Violations[0];location:="{{CHILD_LOCATION}}";field,check:="ParentId","notnull"
 if mode=="supplied zero"{check="refKey"}
 if mode=="unrelated root"{location="{{OTHER_LOCATION}}"}
 if mode=="ordinary Go rule"{field,check="Name","required";location=strings.TrimSuffix(location,"ParentId")+"Name"}
 if v.Location!=location||v.Field!=field||v.Check!=check{t.Fatalf("wrong control violation: %+v want %s/%s/%s",v,location,field,check)}
 t.Logf("control %s: %s at %s; custom=0 queue=0 writes=0",mode,v.Check,v.Location)
}
`
