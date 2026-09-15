package golang

import (
	"fmt"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"strings"
	"testing"
)

func TestExplicitReparentPolicySQLite(t *testing.T) {
	for _, allow := range []bool{false, true} {
		t.Run(fmt.Sprint("allow=", allow), func(t *testing.T) {
			semantic, config := (sequenceValidationFixture{reused: true}).update()
			semantic.Root.Relations[0].AllowReparent = allow
			asset, err := MutationProgram(semantic, config)
			if err != nil {
				t.Fatal(err)
			}
			files, err := asset.Files()
			if err != nil {
				t.Fatal(err)
			}
			source := strings.NewReplacer(`"errors";`, "", ` validate:"gt(50)"`, "", "{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(finalUpdateCoverageSQLiteFixture)
			start := strings.Index(source, " var failed *handler.Validation")
			assertions := ` if err==nil||queueCalls!=0{t.Fatalf("unauthorized reparent: %v queued=%d",err,queueCalls)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name,parent_id FROM records ORDER BY id"},[]struct{Name string;ParentId int64}{{"parent",99},{"child",99}})`
			if allow {
				assertions = ` if err!=nil||queueCalls!=2{t.Fatalf("explicit reparent: %v queued=%d",err,queueCalls)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name,parent_id FROM records ORDER BY id"},[]struct{Name string;ParentId int64}{{"changed parent",99},{"changed child",10}})`
			}
			source = source[:start] + assertions + "\n}\n"
			(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
		})
	}
}

func TestSuppliedIdentityReplacementChecksParentScopeSQLite(t *testing.T) {
	for _, parentID := range []int{10, 98} {
		t.Run(fmt.Sprint("target_parent=", parentID), func(t *testing.T) {
			semantic, config := (sequenceValidationFixture{reused: true}).update()
			semantic.Root.Relations[0].AllowReparent = false
			asset, err := MutationProgram(semantic, config)
			if err != nil {
				t.Fatal(err)
			}
			files, err := asset.Files()
			if err != nil {
				t.Fatal(err)
			}
			source := strings.NewReplacer(`"errors";`, "", ` validate:"gt(50)"`, "", "{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(finalUpdateCoverageSQLiteFixture)
			source = strings.Replace(source, "type Output struct", `func(i *Input)Init(context.Context)error{id:=int64(21);i.Events[0].Children[0].Id=&id;return nil}
type Output struct`, 1)
			source = strings.Replace(source, ` bindings:=[]bindly.BindingSpec`, fmt.Sprintf(` if err:=h.ExecStatements(ctx,"INSERT INTO records VALUES(7,21,%d,'target')");err!=nil{t.Fatal(err)}
 bindings:=[]bindly.BindingSpec`, parentID), 1)
			start := strings.Index(source, " var failed *handler.Validation")
			assertions := ` if err==nil||queueCalls!=0{t.Fatalf("retarget bypassed parent scope: %v queued=%d",err,queueCalls)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records ORDER BY id"},[]struct{Name string}{{"parent"},{"child"},{"target"}})`
			if parentID == 10 {
				assertions = ` if err!=nil||queueCalls!=2{t.Fatalf("scoped replacement: %v queued=%d",err,queueCalls)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name,parent_id FROM records ORDER BY id"},[]struct{Name string;ParentId int64}{{"changed parent",99},{"child",99},{"changed child",10}})`
			}
			source = source[:start] + assertions + "\n}\n"
			(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
		})
	}
}

func TestPartialParentProducedIdentityFreezeSQLite(t *testing.T) {
	for _, self := range []bool{false, true} {
		for _, marked := range []bool{false, true} {
			t.Run(fmt.Sprintf("self=%v/marked=%v", self, marked), func(t *testing.T) {
				fixture := relationProducerFixture{self: self, composite: true, mode: "absent child", rewrite: func(source string) string {
					if !marked {
						source = strings.Replace(source, `child:=&Record{Id:&childID,Name:"child",Has:&Marker{Id:true,Name:true}}`, `child:=&Record{Id:&childID,Name:"child",Has:&Marker{Name:true}}`, 1)
					}
					source = strings.Replace(source, `error{sequenceCalls++;if mode=="reversed order"`, `error{sequenceCalls++;next:=int64(21);parent.Children[0].Id=&next;if mode=="reversed order"`, 1)
					source = strings.Replace(source, ` if mode=="incomplete key"{`, ` if err==nil||!strings.Contains(err.Error(),"frozen resolved identity")||writes.Load()!=0||queueInvocations!=0{t.Fatalf("partial known key changed: %v writes=%d queue=%d",err,writes.Load(),queueInvocations)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,parent_id,name FROM nodes ORDER BY id"},[]struct{Id,ParentId int64;Name string}{{5,5,"anchor"}});return
 if mode=="incomplete key"{`, 1)
					return source
				}}
				fixture.run(t)
			})
		}
	}
}

func TestResolvedIdentityRequiresAllowedMissingPolicy(t *testing.T) {
	fixture := partialIdentityFixture{operation: plan.OperationPatch}
	semantic := fixture.semantic()
	semantic.Root.Write.Allowed = []plan.Action{plan.ActionUpdate}
	asset, err := MutationProgram(semantic, fixture.config(semantic))
	if err == nil || asset != nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("disallowed missing INSERT produced a program: %v", err)
	}
}
