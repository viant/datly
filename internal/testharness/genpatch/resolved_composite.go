package genpatch

import "strings"

var ResolvedCompositeSchema = []string{
	`CREATE TABLE ITEMS(TENANT_ID INTEGER NOT NULL,ORDER_ID INTEGER NOT NULL,ID INTEGER NOT NULL,NAME TEXT,PRIMARY KEY(TENANT_ID,ID),FOREIGN KEY(TENANT_ID,ORDER_ID) REFERENCES ORDERS(TENANT_ID,ID))`,
	`INSERT INTO ITEMS VALUES(1,5,0,'old'),(2,5,0,'outside')`,
}

var ResolvedCompositeDQL = strings.NewReplacer(
	"SELECT o.*,Kinds.*", "SELECT o.*,Items.*,Kinds.*,lifecycle_type(o,'OrderRules'),lifecycle_type(Items,'ItemRules'),CAST(Items.ID AS int),CAST(Items.TENANT_ID AS int),CAST(Items.ORDER_ID AS int)",
	"FROM ORDERS o", "FROM ORDERS o LEFT JOIN ITEMS Items ON Items.TENANT_ID=o.TENANT_ID AND Items.ORDER_ID=o.ID",
).Replace(CompositeDQL)

func ResolvedCompositeRuntime() string {
	source := CompositeRuntimeSource[:strings.Index(CompositeRuntimeSource, " cases:=")]
	source = strings.Replace(source, ` "sort"`, "", 1)
	source = strings.Replace(source, "if err:=db.ExecStatements(ctx,genpatch.CompositeSchema...);err!=nil{t.Fatal(err)}", `if err:=db.ExecStatements(ctx,genpatch.CompositeSchema...);err!=nil{t.Fatal(err)}
 if err:=db.ExecStatements(ctx,genpatch.ResolvedCompositeSchema...);err!=nil{t.Fatal(err)}`, 1)
	return source + `
 if _,err=invoke(` + "`" + `{"Data":[{"tenantId":1,"id":5,"Items":[{"name":"old"}]}]}` + "`" + `);err!=nil{t.Fatal(err)}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT TENANT_ID,ID,NAME FROM ITEMS ORDER BY TENANT_ID"},[]struct{TenantId,Id int;Name string}{{1,0,"resolved"},{2,0,"outside"}})
 for _,body:=range []string{
 ` + "`" + `{"Data":[{"tenantId":1,"id":5,"name":"must rollback","Items":[{"name":"unresolved"}]}]}` + "`" + `,
 ` + "`" + `{"Data":[{"tenantId":1,"id":5,"name":"must rollback","Items":[{"tenantId":2,"id":0,"name":"wrong parent"}]},{"tenantId":2,"id":5}]}` + "`" + `,
 }{
  if _,err=invoke(body);err==nil{t.Fatal("absent zero or cross-parent identity authorized")}
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ORDERS ORDER BY TENANT_ID"},[]struct{Name string}{{"a"},{"b"}})
  db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM ITEMS ORDER BY TENANT_ID"},[]struct{Name string}{{"resolved"},{"outside"}})
 }
 if _,err=invoke(` + "`" + `{"Data":[{"tenantId":3,"id":5,"name":"new parent","Items":[{"name":"new zero"}]}]}` + "`" + `);err!=nil{t.Fatal(err)}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT TENANT_ID,ORDER_ID,ID,NAME FROM ITEMS WHERE TENANT_ID=3"},[]struct{TenantId,OrderId,Id int;Name string}{{3,5,0,"new zero"}})
}
func(input *OrdersInput)Init(ctx context.Context)error{
 indexes,err:=input.ReadIndexes(ctx);if err!=nil{return err}
 keyed:=indexes.CurrentItemsByKey
 grouped:=indexes.CurrentItems.GroupById()
 if len(indexes.CurrentItems)==2 {
  if len(grouped[0])!=2||!keyed.Has(OrdersHandlerCurrentItemsKey{TenantId:1,Id:0})||!keyed.Has(OrdersHandlerCurrentItemsKey{TenantId:2,Id:0}){return fmt.Errorf("composite or grouped helper lost zero tuples")}
  if _,err=indexes.CurrentItems.IndexById();err==nil{return fmt.Errorf("nonunique keyed helper accepted")}
 }
 for _,order:=range input.Orders{for _,item:=range order.Items{
  if item.Name==nil{continue}
  switch *item.Name {
   case "old":
    rows:=indexes.CurrentItems.GroupByName()[*item.Name];if len(rows)!=1{return fmt.Errorf("composite natural match ambiguous or outside scope")}
    item.SetId(rows[0].Id);item.SetTenantId(rows[0].TenantId);name:="resolved";item.SetName(&name)
   case "new zero":item.SetId(0);item.SetTenantId(3)
  }
 }}
 return nil
}
`
}
