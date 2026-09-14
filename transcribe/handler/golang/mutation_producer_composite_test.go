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
		source = strings.Replace(source, ` if mode=="supplied nil"{`, ` if mode=="parent update"{if err==nil||!strings.Contains(err.Error(),"partial original identity")||initCalls!=0||customCalls!=0||sequenceCalls!=0||queueCalls!=0||writes.Load()!=0{t.Fatalf("UPDATE parent authorized a missing composite key part: %v",err)};return}
 if mode=="supplied nil"{`, 1)
	}

	return source
}
