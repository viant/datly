package golang

import (
	"strings"
	"testing"
)

func TestRelationProducerCompositeSQLite(t *testing.T) {
	for _, self := range []bool{false, true} {
		layout := "relation"
		if self {
			layout = "self"
		}
		t.Run(layout, func(t *testing.T) {
			for _, mode := range []string{"absent child", "partial current", "parent update", "produced Go", "produced Go failure", "incomplete key", "supplied nil", "supplied zero", "supplied conflict", "altered topology", "reversed order"} {
				t.Run(mode, func(t *testing.T) { (relationProducerFixture{self: self, mode: mode, composite: true}).run(t) })
			}
		})
	}
}

func (f relationProducerFixture) refineSource(source string) string {
	if strings.HasPrefix(f.mode, "produced Go") {
		source = strings.Replace(source, `"context";`, `"github.com/viant/govalidator";"context";`, 1)
		source = strings.Replace(source, `validate:"required"`+"`;Children", `validate:"required,produced_fk"`+"`;Children", 1)
		source += `
func init(){govalidator.RegisterWithDependencies("produced_fk",func(_ *govalidator.Field,_ *govalidator.Check)(govalidator.IsValid,error){
 return func(ctx context.Context,_ any)(bool,error){
  row:=ctx.Value(govalidator.SessionKey).(*govalidator.Session).ParentValue.(Record)
  if row.Name!="child"{return true,nil};if sequenceCalls==0{return false,errors.New("produced Go rule ran before its captured FK was available")}
  if row.ParentId==nil||*row.ParentId!=6{return false,errors.New("produced Go rule did not observe the final FK")}
  return mode!="produced Go failure",nil
 },nil
},func(*govalidator.Field,*govalidator.Check)([]string,error){return []string{"ParentId"},nil})}
`
	}
	if f.velty {
		source = f.veltySource(source)
	}
	if f.callerTx {
		source = f.transactionSource(source)
	}
	if !f.composite {
		return source
	}
	source = strings.NewReplacer(
		"Id,ParentId,Name,Children bool", "Id,ParentId,AncestorId,Name,Children bool",
		"type Record struct{", "type Record struct{AncestorId *int64 `sqlx:\"ancestor_id,required,refTable=nodes,refColumn=parent_id\"`;",
		`sqlx:"parent_id,required,`, `sqlx:"parent_id,primaryKey,required,`,
		"id INTEGER PRIMARY KEY,parent_id INTEGER NOT NULL,name TEXT NOT NULL,UNIQUE(parent_id,name),FOREIGN KEY(parent_id) REFERENCES nodes(id)",
		"id INTEGER NOT NULL,parent_id INTEGER NOT NULL,name TEXT NOT NULL,ancestor_id INTEGER NOT NULL,PRIMARY KEY(id,parent_id),UNIQUE(parent_id,name),FOREIGN KEY(parent_id,ancestor_id) REFERENCES nodes(id,parent_id)",
		"INSERT INTO nodes VALUES(5,5,'anchor')", "INSERT INTO nodes VALUES(5,5,'anchor',5)",
		"INSERT INTO nodes VALUES(99,999,'orphan')", "INSERT INTO nodes VALUES(99,6,'orphan',999)",
		`parent=&Record{ParentId:&anchor,Name:"parent",Has:&Marker{ParentId:true,Name:true}}`,
		`parent=&Record{ParentId:&anchor,AncestorId:&anchor,Name:"parent",Has:&Marker{ParentId:true,AncestorId:true,Name:true}}`,
	).Replace(source)
	// The real composite FK must have both produced parts before Queue. An orphan
	// using a valid first part and invalid second part must still fail in SQLite.
	source = strings.Replace(source, `func(p *observedProgram)Queue(ctx context.Context)error{`, `func(p *observedProgram)Queue(ctx context.Context)error{if parent.Children[0].AncestorId==nil||*parent.Children[0].AncestorId!=5{return errors.New("composite relation is incomplete before Queue")};`, 1)
	if f.mode == "partial current" {
		source = strings.NewReplacer(
			`if mode=="child update"{if err:=h.ExecStatements(ctx,"INSERT INTO nodes VALUES(20,5,'old child')")`,
			`if mode=="partial current"{if err:=h.ExecStatements(ctx,"INSERT INTO nodes VALUES(20,5,'old child',5)")`,
			`expectedID:=int64(6);`, `expectedID:=int64(21);`,
			`{{5,5,"anchor"},{6,5,"parent"},{20,6,"child"}}`, `{{5,5,"anchor"},{20,5,"old child"},{20,21,"child"},{21,5,"parent"}}`,
			`FROM nodes ORDER BY id`, `FROM nodes ORDER BY id,parent_id`,
		).Replace(source)
		// Concrete Previous assertions use each generated hook's existing typed state.
		source = strings.Replace(source, `if state.Parent!=parent||state.SelfParent!=nil`, `if state.Previous!=nil{return errors.New("partial original key matched Previous by a prefix")};if state.Parent!=parent||state.SelfParent!=nil`, 1)
		source = strings.Replace(source, `initCalls++
 if row!=parent`, `initCalls++;if state.Previous!=nil{return errors.New("partial original key matched Previous by a prefix")}
 if row!=parent`, 1)
	}
	// Supplied invalid identity parts are rejected by the original identity owner;
	// they must not become full-key matches or get sequencing/queue privileges.
	source = strings.Replace(source, ` if mode=="incomplete key"{`, ` if mode=="supplied nil"{if err==nil||!strings.Contains(err.Error(),"supplied without a value")||customCalls!=0||sequenceCalls!=0||queueCalls!=0||writes.Load()!=0{t.Fatalf("supplied null composite identity accepted: %v",err)};return}
 if mode=="incomplete key"{`, 1)
	if f.mode == "parent update" {
		childAction := "captured.actions.role1[0].Action"
		if f.self {
			childAction = "captured.actions.role0[1].Action"
		}
		source = strings.Replace(source, `func(p *observedProgram)Queue(ctx context.Context)error{`, `func(p *observedProgram)Queue(ctx context.Context)error{if captured.actions.role0[0].Action!=handler.WriteUpdate||`+childAction+`!=handler.WriteInsert{return errors.New("parent-produced composite actions changed")};`, 1)
		source = strings.Replace(source, ` if mode=="supplied nil"{`, ` if mode=="parent update"{
  if err!=nil||queueInvocations!=1||queueCalls!=2||writes.Load()!=2{t.Fatalf("stable UPDATE parent did not supply a new INSERT child: %v queue=%d hooks=%d writes=%d",err,queueInvocations,queueCalls,writes.Load())}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,ancestor_id,name FROM nodes ORDER BY id"},[]struct{Id,ParentId,AncestorId int64;Name string}{{5,5,5,"parent"},{20,5,5,"child"}})
 }
 if mode=="supplied nil"{`, 1)
	}
	if f.mode == "supplied conflict" {
		source = strings.Replace(source, `!strings.Contains(err.Error(),"supplied relation field conflicts")`, `!strings.Contains(err.Error(),"frozen resolved identity")`, 1)
		source = strings.Replace(source, `t.Fatalf("supplied link was overwritten: %v",err)}
  return`, `t.Fatalf("supplied link was overwritten: %v",err)}
  if queueInvocations!=0{t.Fatal("conflicting composite reached Queue")}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,ancestor_id,name FROM nodes ORDER BY id"},[]struct{Id,ParentId,AncestorId int64;Name string}{{5,5,5,"anchor"}})
  return`, 1)
	}

	return source
}

func TestCompositeParentProducedCollisionRemainsInsert(t *testing.T) {
	for _, self := range []bool{false, true} {
		name := "relation"
		if self {
			name = "self"
		}
		t.Run(name, func(t *testing.T) {
			fixture := relationProducerFixture{self: self, composite: true, mode: "parent update", rewrite: func(source string) string {
				source = strings.Replace(source, ` connection,err:=h.DB.Conn(ctx)`, ` if err:=h.ExecStatements(ctx,"INSERT INTO nodes VALUES(20,5,'existing child',5)");err!=nil{t.Fatal(err)}
 connection,err:=h.DB.Conn(ctx)`, 1)
				start := strings.Index(source, ` if mode=="parent update"{`)
				end := strings.Index(source[start:], ` if mode=="supplied nil"{`) + start
				if start < 0 || end < start {
					t.Fatal("parent proof block missing")
				}
				source = source[:start] + ` if mode=="parent update"{
    if err==nil||queueInvocations!=1{t.Fatalf("pending child did not retain INSERT through Queue: %v queue=%d",err,queueInvocations)}
    h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,ancestor_id,name FROM nodes ORDER BY id"},[]struct{Id,ParentId,AncestorId int64;Name string}{{5,5,5,"anchor"},{20,5,5,"existing child"}})
    if len(outcomes)!=1||outcomes[0].CommitConfirmed(){t.Fatalf("collision committed: %v %+v",err,outcomes)}
    return
   }
` + source[end:]
				return source
			}}
			fixture.run(t)
		})
	}
}
