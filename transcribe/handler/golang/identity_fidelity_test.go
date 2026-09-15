package golang

import (
	"strings"
	"testing"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// The independent probe's inputs, scope and SQLite trigger assertions are
// retained in /tmp/datly-identity-correction-probes/original. This executable
// version requires the corrected behavior, including partial-key freezes.
func TestCorrectedIndependentIdentityFidelity(t *testing.T) {
	fixture := partialIdentityFixture{operation: plan.OperationPatch}
	semantic := fixture.semantic()
	asset, err := MutationProgram(semantic, fixture.config(semantic))
	if err != nil {
		t.Fatal(err)
	}
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	source := partialIdentitySQLiteFixture
	start := strings.Index(source, "func(i *Input)Init(")
	end := strings.Index(source[start:], "type Hooks struct") + start
	source = source[:start] + `var inputCalls int
func(i *Input)Init(context.Context)error{
 inputCalls++
 switch i.Mode {
 case "replace supplied":id:=int64(10);i.Events[0].Id=&id;i.Events[0].TenantId=5
 case "raw resolved","duplicate resolved":for _,row:=range i.Events{id:=int64(10);row.Id=&id}
 case "sequence retarget","entity retarget","replace supplied missing","replace supplied outside":id:=int64(50);i.Events[0].Id=&id;i.Events[0].TenantId=5
 }
 return nil
}
` + source[end:]
	start = strings.Index(source, " if state.Previous!=nil{return fmt.Errorf(")
	end = strings.Index(source[start:], " return nil\n}") + start
	source = source[:start] + ` wantPrevious:=h.Input.Mode=="replace supplied"||h.Input.Mode=="raw resolved"||h.Input.Mode=="duplicate resolved"||h.Input.Mode=="pointer present"||h.Input.Mode=="scalar present"||h.Input.Mode=="pointer zero present"||h.Input.Mode=="update retarget"
 if (state.Previous!=nil)!=wantPrevious{return fmt.Errorf("wrong scoped match in %s: %+v",h.Input.Mode,state.Previous)}
 if h.Input.Mode=="replace supplied" {
  original:=state.Original.(*_newEventsHandlerOriginal0)
  if *state.Previous.Id!=10||state.Previous.TenantId!=5||*row.Id!=10||row.TenantId!=5||original.keyId!=0||original.keyTenantId!=0||!original.Has("Id")||!original.Has("TenantId"){return fmt.Errorf("effective/original tuple facts conflated")}
 }
 if h.Input.Mode=="pointer present"&&state.Original.Has("Id"){return fmt.Errorf("precomputed pointer changed original marker")}
 if h.Input.Mode=="scalar present"&&state.Original.Has("TenantId"){return fmt.Errorf("precomputed scalar changed original marker")}
 if h.Input.Mode=="entity pending retarget"{id:=int64(50);row.SetId(&id)}
 if h.Input.Mode=="entity retarget"{id:=int64(60);row.SetId(&id)}
` + source[end:]
	source = strings.Replace(source, `sequenced++;if h.Input.Mode=="changed tenant"{row.TenantId=92;row.Has.TenantId=false}`, `sequenced++
 switch h.Input.Mode {
 case "sequence retarget","entity pending retarget":id:=int64(60);row.SetId(&id)
 case "partial known retarget","partial precomputed retarget":row.TenantId=6
 case "update retarget":id:=int64(10);row.Id=&id;row.TenantId=5
 }`, 1)
	source = strings.Replace(source, `"tenant zero","tenant seven","changed tenant","missing tenant","supplied nil","business failure"`, `"pointer present","pointer absent","pointer zero present","pointer zero absent","scalar present","scalar absent","absent scalar zero","replace supplied","raw resolved","sequence retarget","duplicate resolved","partial known retarget","partial precomputed retarget","entity retarget","entity pending retarget","update retarget","replace supplied missing","replace supplied outside"`, 1)
	source = strings.Replace(source, "initialized=0;validated=0;sequenced=0;queued=0", "inputCalls=0;initialized=0;validated=0;sequenced=0;queued=0", 1)
	source = strings.Replace(source, `  dependencies,err:=compiler.CompileViewDependencies`, `  if mode=="replace supplied outside"{component.Views[0].Source.SQL+=" WHERE id<>50"}
  dependencies,err:=compiler.CompileViewDependencies`, 1)
	source = strings.Replace(source, `  definition:={{FACTORY}}().(*{{DEFINITION}})`, `  switch mode {
  case "pointer present","pointer absent":id:=int64(50);row.Id=&id;row.TenantId=5
  case "pointer zero present","pointer zero absent":id:=int64(0);row.Id=&id
  case "scalar present","scalar absent":id:=int64(50);row.Id=&id;row.Has.Id=true;row.TenantId=5;row.Has.TenantId=false
  case "absent scalar zero":id:=int64(0);row.Id=&id;row.Has.Id=true;row.Has.TenantId=false
  case "replace supplied","replace supplied missing","replace supplied outside","update retarget":id:=int64(0);row.Id=&id;row.Has.Id=true
  case "raw resolved","duplicate resolved":row.TenantId=5
  case "partial precomputed retarget":row.TenantId=5;row.Has.TenantId=false
  }
  if mode=="pointer present"||mode=="scalar present"||mode=="replace supplied outside"{if err=h.ExecStatements(ctx,"INSERT INTO records VALUES(5,50,'preexisting')","DELETE FROM trace");err!=nil{t.Fatal(err)}}
  if mode=="pointer zero absent"{if err=h.ExecStatements(ctx,"DELETE FROM records WHERE tenant_id=0 AND id=0","DELETE FROM trace");err!=nil{t.Fatal(err)}}
  events:=[]*Record{row};if mode=="duplicate resolved"{events=append(events,&Record{TenantId:5,Name:"second",Has:&Marker{TenantId:true,Name:true}})}
  definition:={{FACTORY}}().(*{{DEFINITION}})`, 1)
	source = strings.Replace(source, `"events":[]*Record{row}`, `"events":events`, 1)
	start = strings.Index(source, `  if mode=="changed tenant"&&`)
	end = strings.Index(source[start:], "\n })}\n}") + start
	source = source[:start] + `  fail:=mode=="entity pending retarget"||mode=="absent scalar zero"||mode=="sequence retarget"||mode=="partial known retarget"||mode=="partial precomputed retarget"||mode=="entity retarget"||mode=="update retarget"||mode=="duplicate resolved"||mode=="replace supplied outside"
 if (err!=nil)!=fail||inputCalls!=1{t.Fatalf("mode=%s error=%v input calls=%d",mode,err,inputCalls)}
 if fail {
  if strings.Contains(mode,"retarget")&&!strings.Contains(err.Error(),"frozen resolved identity"){t.Fatalf("wrong freeze failure: %v",err)}
  if mode=="absent scalar zero"&&!strings.Contains(err.Error(),"partial resolved identity"){t.Fatalf("absent scalar zero gained a match: %v",err)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM trace"},[]struct{N int}{{0}})
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records WHERE tenant_id=0 AND id=0"},[]struct{Name string}{{"stored zero"}})
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records WHERE tenant_id=5 AND id=10"},[]struct{Name string}{{"highest"}})
  return
 }
 action:="insert";tenant,id:=int64(5),int64(50)
 switch mode {
 case "replace supplied","raw resolved":action="update";id=10
 case "pointer present","scalar present":action="update"
 case "pointer zero present":action="update";tenant=0;id=0
 case "pointer zero absent":tenant=0;id=0
 }
 if initialized!=1||validated!=1||sequenced!=1||queued!=1{t.Fatalf("lifecycle counts %d/%d/%d/%d",initialized,validated,sequenced,queued)}
 if row.Id==nil||*row.Id!=id||row.TenantId!=tenant{t.Fatalf("effective tuple changed: %+v",row)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT action FROM trace"},[]struct{Action string}{{action}})
 h.AssertQuery(t,ctx,sqlite.Query{SQL:fmt.Sprintf("SELECT name FROM records WHERE tenant_id=%d AND id=%d",tenant,id)},[]struct{Name string}{{"inserted"}})
 if mode=="replace supplied"||mode=="replace supplied missing"{h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records WHERE tenant_id=0 AND id=0"},[]struct{Name string}{{"stored zero"}})}
` + source[end:]
	source = strings.NewReplacer("{{OPERATION}}", "PATCH", "{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(source)
	(entitySyncFixture{entity: asset.Entities, products: files, source: source}).run(t)
}
