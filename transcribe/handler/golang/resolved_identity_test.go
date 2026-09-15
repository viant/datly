package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestResolvedCompositeIdentityLifecycleSQLite(t *testing.T) {
	for _, linked := range []bool{false, true} {
		name := "Input.Init"
		if linked {
			name = "linked adapter"
		}
		t.Run(name, func(t *testing.T) { resolvedIdentityLifecycle(t, linked) })
	}
}
func resolvedIdentityLifecycle(t *testing.T, linked bool) {
	fixture := partialIdentityFixture{operation: plan.OperationPatch}
	semantic := fixture.semantic()
	config := fixture.config(semantic)
	if linked {
		read := plan.ReadCollection{Name: "CurrentEvents", InputPath: semantic.Root.Current.InputPath, Type: spec.TypeRef{Name: "Previous"}, Keys: semantic.Root.Current.Keys}
		for _, field := range semantic.Root.Current.Fields {
			read.Fields = append(read.Fields, field.Current)
		}
		semantic.ReadCollections = []plan.ReadCollection{read}
		config.ReadIndexes = &ReadIndexConfig{Package: config.Package, PackagePath: config.PackagePath, InputType: config.InputType}
	}
	asset, err := MutationProgram(semantic, config)
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
	// Reuse the existing complete engine/SQLite fixture and module owner.
	source := strings.Replace(partialIdentitySQLiteFixture, `;"strings"`, "", 1)
	start := strings.Index(source, "func(i *Input)Init(")
	end := strings.Index(source[start:], "type Hooks struct") + start
	source = source[:start] + `func(i *Input)Init(context.Context)error{
 row:=i.Events[0]
 switch i.Mode {
 case "resolved zero": zero:=int64(0);row.SetId(&zero)
 case "resolved scalar zero": row.SetTenantId(0)
 case "new preallocated": next:=int64(50);row.SetId(&next)
 }
 return nil
}
` + source[end:]
	start = strings.Index(source, " if state.Previous!=nil{return fmt.Errorf(")
	end = strings.Index(source[start:], " return nil\n}") + start
	source = source[:start] + ` update:=h.Input.Mode=="resolved zero"||h.Input.Mode=="resolved scalar zero"||h.Input.Mode=="original existing"
 if (state.Previous!=nil)!=update{return fmt.Errorf("wrong classification in Init mode=%s previous=%+v",h.Input.Mode,state.Previous)}
 if state.Previous!=nil&&(*state.Previous.Id!=0||state.Previous.TenantId!=0){return fmt.Errorf("wrong composite match")}
 if h.Input.Mode=="resolved zero"&&state.Original.Has("Id"){return fmt.Errorf("resolved ID contaminated original presence")}
 if h.Input.Mode=="resolved scalar zero"&&state.Original.Has("TenantId"){return fmt.Errorf("resolved zero contaminated original marker")}
` + source[end:]
	source = strings.Replace(source, `sequenced++;if h.Input.Mode=="changed tenant"{row.TenantId=92;row.Has.TenantId=false}`, `sequenced++;if h.Input.Mode=="sequence collision"{zero:=int64(0);row.SetId(&zero)}`, 1)
	source = strings.Replace(source, `"tenant zero","tenant seven","changed tenant","missing tenant","supplied nil","business failure"`, `"resolved zero","resolved scalar zero","new preallocated","sequence collision","unresolved scalar zero","original existing","missing supplied"`, 1)
	needle := `  definition:={{FACTORY}}().(*{{DEFINITION}})`
	source = strings.Replace(source, needle, `  switch mode {
  case "resolved scalar zero","unresolved scalar zero":zero:=int64(0);row.Id=&zero;row.Has.Id=true;row.Has.TenantId=false
  case "original existing":zero:=int64(0);row.Id=&zero;row.Has.Id=true
  case "missing supplied":next:=int64(30);row.Id=&next;row.Has.Id=true
  }
`+needle, 1)
	start = strings.Index(source, `  if mode=="changed tenant"&&`)
	end = strings.Index(source[start:], "\n })}\n}") + start
	source = source[:start] + `  success:=mode!="sequence collision"&&mode!="unresolved scalar zero"
  if (err==nil)!=success{t.Fatalf("mode=%s error=%v",mode,err)}
  if !success {
   h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM trace"},[]struct{N int}{{0}})
   h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records WHERE tenant_id=0 AND id=0"},[]struct{Name string}{{"stored zero"}})
   return
  }
  update:=mode=="resolved zero"||mode=="resolved scalar zero"||mode=="original existing"
  action:="insert";if update{action="update"}
  if initialized!=1||validated!=1||sequenced!=1||queued!=1{t.Fatalf("lifecycle counts %d/%d/%d/%d",initialized,validated,sequenced,queued)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT action FROM trace"},[]struct{Action string}{{action}})
  expected:=int64(0);if mode=="new preallocated"{expected=50};if mode=="missing supplied"{expected=30}
  if row.Id==nil||*row.Id!=expected||row.TenantId!=0{t.Fatalf("resolved identity changed: %+v",row)}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT name FROM records WHERE tenant_id=0 AND id="+fmt.Sprint(expected)},[]struct{Name string}{{"inserted"}})
` + source[end:]
	if linked {
		source = strings.Replace(source, "switch i.Mode {", `switch "" {`, 1)
		source = strings.Replace(source, `  definition:={{FACTORY}}().(*{{DEFINITION}})`, `  definition:={{FACTORY}}().(*{{DEFINITION}})
  definition.ResolveIdentity=func(ctx context.Context,input *Input,indexes *EventsHandlerReadIndexes)error{
   byName,err:=indexes.CurrentEvents.IndexByName();if err!=nil{return err}
   if !byName.Has("stored zero"){return fmt.Errorf("typed linked lookup unavailable")}
   keyed,err:=indexes.CurrentEvents.IndexByKey();if err!=nil{return err}
   if !keyed.Has(EventsHandlerCurrentEventsKey{Id:0,TenantId:0})||len(indexes.CurrentEvents.GroupByKey()[EventsHandlerCurrentEventsKey{Id:0,TenantId:0}])!=1{return fmt.Errorf("typed composite zero index unavailable")}
   target:=input.Events[0]
   switch input.Mode {
    case "resolved zero":id:=*byName["stored zero"].Id;target.SetId(&id)
    case "resolved scalar zero":target.SetTenantId(byName["stored zero"].TenantId)
    case "new preallocated":id:=int64(50);target.SetId(&id)
   }
   return nil
  }`, 1)
	}
	source = strings.NewReplacer("{{OPERATION}}", "PATCH", "{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(source)
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}
