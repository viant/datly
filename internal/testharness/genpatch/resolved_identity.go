package genpatch

import "strings"

var ResolvedIdentitySchema = []string{
	`INSERT INTO ORDERS VALUES(2,7,'outside','2026-09-01T00:00:00Z','2026-09-30T00:00:00Z')`,
	`INSERT INTO ITEMS VALUES(20,2,'old')`,
}

var DeepIdentitySchema = []string{
	`CREATE TABLE DETAILS(ID INTEGER PRIMARY KEY AUTOINCREMENT,ITEM_ID INTEGER NOT NULL REFERENCES ITEMS(ID),NOTE TEXT NOT NULL)`,
	`INSERT INTO ITEMS VALUES(30,1,'hidden')`,
	`INSERT INTO DETAILS VALUES(100,10,'old detail'),(200,20,'old detail'),(300,10,'hidden'),(301,30,'visible under hidden child')`,
}

// NamedIdentityDQL uses only authored application shape and scope. GEN derives
// the body, key projections, Previous reads and FK-scoped child reads.
var NamedIdentityDQL = strings.NewReplacer(
	"FROM ORDERS o", "FROM (SELECT base.* FROM ORDERS base WHERE base.ID<>2) o",
	"JOIN ITEMS Items", "LEFT JOIN (SELECT base.* FROM ITEMS base WHERE base.NAME<>'hidden') Items",
	"SELECT o.*, Items.*, Kinds.*,", "SELECT o.*, Items.*, Details.*, Kinds.*, lifecycle_type(Details,'DetailRules'),",
	"JOIN (ORDER_KINDS) Kinds", "LEFT JOIN (SELECT d.* FROM DETAILS d WHERE d.NOTE<>'hidden') Details ON Details.ITEM_ID=Items.ID LEFT JOIN (SELECT k.* FROM (ORDER_KINDS) k WHERE k.ID=7) Kinds",
).Replace(LifecycleDQL)

// ResolvedIdentityRuntime reuses the shared real registration, resource, reader,
// transaction and HTTP fixture; only the application callbacks/cases differ.
func ResolvedIdentityRuntime(deep bool) string {
	source := strings.Replace(RuntimeSource, "func(input *OrdersInput)Init(context.Context)error{", `func(input *OrdersInput)Init(ctx context.Context)error{
 if input._ordersHandlerReadIndexes==nil{return fmt.Errorf("indexes were not prepared before Input.Init")}
 indexes,err:=input.ReadIndexes(ctx);if err!=nil{return err}
 for _,name:=range []string{"CurrentOrdersGroupedByName","CurrentOrdersGroupedByStart","CurrentOrdersGroupedByEnd","CurrentItemsGroupedByName","CurrentKindsGroupedByName"}{if _,exists:=reflect.TypeOf(*indexes).FieldByName(name);exists{return fmt.Errorf("optional business group eagerly exposed: %s",name)}}
 if len(indexes.CurrentItems)>0&&len(indexes.CurrentItemsGroupedByOrderId)==0{return fmt.Errorf("canonical relationship group was not prepared")}
 again,err:=input.ReadIndexes(ctx);if err!=nil||again!=indexes{return fmt.Errorf("read indexes not invocation cached")}
 kinds:=indexes.CurrentKindsById
 if len(input.CurrentOrders)>0&&!kinds.Has(7)&&len(input.CurrentKinds)>0{return fmt.Errorf("typed auxiliary index missing")}
 byName:=indexes.CurrentItems.GroupByName()
 for _,order:=range input.Orders {for _,item:=range order.Items {
  if item.Id==nil && item.Name!=nil {
   switch *item.Name {
   case "old","ambiguous":
    matches:=byName[*item.Name];if len(matches)!=1{
     if _,keyErr:=indexes.CurrentItems.IndexByName();len(matches)>1&&keyErr==nil{return fmt.Errorf("duplicate keyed index accepted")}
     return fmt.Errorf("ambiguous or missing natural identity")
    }
    resolved:=*matches[0].Id;item.SetId(&resolved);name:="updated";item.SetName(&name)
    // This public helper has detached rows. Poisoning it cannot change Previous.
    *matches[0].Id=777
   case "preallocated":next:=90;item.SetId(&next)
   case "forged":
    keyed,err:=indexes.CurrentItems.IndexById();if err!=nil{return err}
    keyed[20]=keyed[10];next:=20;item.SetId(&next)
   }
  }
  {{DEEP_INIT}}
 }}
 `, 1)
	source = strings.Replace(source, `{"id":10,"name":"updated"}`, `{"name":"old"{{DEEP_BODY}}}`, 1)
	source = strings.Replace(source, "if err:=db.ExecStatements(ctx,genpatch.Schema...);err!=nil{t.Fatal(err)}", `if err:=db.ExecStatements(ctx,genpatch.Schema...);err!=nil{t.Fatal(err)}
 if err:=db.ExecStatements(ctx,genpatch.ResolvedIdentitySchema...);err!=nil{t.Fatal(err)}
 {{DEEP_SCHEMA}}`, 1)
	// Independent out-of-scope records are intentionally present in the DB.
	source = strings.ReplaceAll(source, "SELECT NAME FROM ITEMS ORDER BY ID", "SELECT NAME FROM ITEMS WHERE ORDER_ID=1 ORDER BY ID")
	suffix := `
 {{DEEP_ASSERT}}
 actual,err=invoke(` + "`" + `{"Data":[{"id":1,"Items":[{"name":"preallocated"}]}]}` + "`" + `)
 if err!=nil{t.Fatal(err)}
 if got:=actual.(*OrdersOutput).Data[0].Items[0];got.Id==nil||*got.Id!=90{t.Fatalf("preallocated identity was sequenced: %+v",got)}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=90"},[]struct{Name string}{{"preallocated"}})
 for _,body:=range []string{
  ` + "`" + `{"Data":[{"id":1,"name":"must rollback","Items":[{"id":20,"name":"out of scope"}]}]}` + "`" + `,
  ` + "`" + `{"Data":[{"id":1,"name":"must rollback","Items":[{"name":"forged"}]}]}` + "`" + `,
 }{
  if _,err=invoke(body);err==nil{t.Fatal("out-of-scope identity was authorized")}
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=20"},[]struct{Name string}{{"old"}})
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ORDERS WHERE ID=1"},[]struct{Name string}{{"before"}})
 }
 if !resolvedChildSeen||!preallocatedChildSeen||groupedValidated==0{t.Fatalf("lifecycle proof missing: resolved=%v preallocated=%v validated=%d",resolvedChildSeen,preallocatedChildSeen,groupedValidated)}
 if err=db.ExecStatements(ctx,"INSERT INTO ITEMS VALUES(91,1,'ambiguous'),(92,1,'ambiguous')");err!=nil{t.Fatal(err)}
 if _,err=invoke(` + "`" + `{"Data":[{"id":1,"name":"must rollback","Items":[{"name":"ambiguous"}]}]}` + "`" + `);err==nil||!strings.Contains(err.Error(),"ambiguous"){t.Fatalf("ambiguous lookup = %v",err)}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ORDERS WHERE ID=1"},[]struct{Name string}{{"before"}})
`
	source = strings.Replace(source, "\n}\n\nfunc TestGeneratedSettersAndSyncPresence", suffix+"\n}\n\nfunc TestGeneratedSettersAndSyncPresence", 1)
	deepInit, deepBody, deepSchema, deepAssert := "", "", "", ""
	if deep {
		deepInit = `if indexes.CurrentItemsById.Has(30)||indexes.CurrentDetailsById.Has(300)||indexes.CurrentDetailsById.Has(301){return fmt.Errorf("authored inner filter did not bound Previous")}
 for _,detail:=range item.Details{if detail.Id==nil&&detail.Note!=nil&&*detail.Note=="old detail"{
    matches:=indexes.CurrentDetails.GroupByNote()[*detail.Note];if len(matches)!=1{return fmt.Errorf("deep scope lookup ambiguous or missing")}
    detail.SetId(matches[0].Id);note:="resolved detail";detail.SetNote(&note)
   }}`
		deepBody = `,"Details":[{"note":"old detail"}]`
		deepSchema = `if err:=db.ExecStatements(ctx,genpatch.DeepIdentitySchema...);err!=nil{t.Fatal(err)}`
		deepAssert = `db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NOTE FROM DETAILS ORDER BY ID"},[]struct{Note string}{{"resolved detail"},{"old detail"},{"hidden"},{"visible under hidden child"}})
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS WHERE ID=30"},[]struct{Name string}{{"hidden"}})`
	}
	if deep {
		source = strings.ReplaceAll(source, "SELECT NAME FROM ITEMS WHERE ORDER_ID=1 ORDER BY ID", "SELECT NAME FROM ITEMS WHERE ORDER_ID=1 AND NAME<>'hidden' ORDER BY ID")
	}
	return strings.NewReplacer("{{DEEP_INIT}}", deepInit, "{{DEEP_BODY}}", deepBody, "{{DEEP_SCHEMA}}", deepSchema, "{{DEEP_ASSERT}}", deepAssert).Replace(source)
}
