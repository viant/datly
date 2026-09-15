package genpatch

import (
	"fmt"
	"strings"
)

// PreparationWideSchema adds declared business fields; both baseline and final
// benchmarks validate their evidence and clone their values identically.
func PreparationWideSchema() []string {
	var result []string
	for i := 0; i < 16; i++ {
		result = append(result, fmt.Sprintf("ALTER TABLE ORDERS ADD COLUMN EXTRA%d TEXT", i))
	}
	return result
}

// PreparationCostRuntime measures the same real-reader-bound workload on the
// old and selected-group defaults. The diagnostic passes share native clone,
// read evidence, and generated typed constructor owners with normal preparation.
func PreparationCostRuntime(wideOptions ...bool) string {
	wide := len(wideOptions) > 0 && wideOptions[0]
	source := ResolvedIdentityRuntime(true)
	start, end := strings.Index(source, "var lookupRead bool"), strings.Index(source, "func TestGeneratedPatchRuntime")
	source = source[:start] + `var measuredInput *OrdersInput
var measuredContext context.Context
var preparedIndexSink *OrdersHandlerReadIndexes
var capturedProgramSink any
var groupSink any
func(input *OrdersInput)Init(ctx context.Context)error{measuredInput=input;measuredContext=ctx;return fmt.Errorf("cost capture stop")}
` + source[end:]
	source = source[:strings.Index(source, " actual,err:=invoke(")]
	source = strings.Replace(source, ` "time"`, "", 1)
	source = strings.Replace(source, ` "github.com/viant/xdatly/response"`, "", 1)
	source = strings.Replace(source, ` "context"`, ` "context";"encoding/json";sdk "github.com/viant/xdatly/handler";shape "github.com/viant/x/shape"`, 1)
	source = strings.Replace(source, "func TestGeneratedPatchRuntime(t *testing.T)", "func benchmarkPreparedGraph(t *testing.B,parentCount int)", 1)
	if wide {
		source = strings.Replace(source, ` holder:=reflect.TypeOf`, ` for n:=0;n<16;n++{if err:=db.ExecStatements(ctx,fmt.Sprintf("ALTER TABLE ORDERS ADD COLUMN EXTRA%d TEXT",n));err!=nil{t.Fatal(err)}}
 holder:=reflect.TypeOf`, 1)
	}
	result := source + `
 type detailRequest struct{Id int ` + "`json:\"id\"`" + `}
 type itemRequest struct{Id int ` + "`json:\"id\"`" + `;Details []detailRequest}
 type orderRequest struct{Id int ` + "`json:\"id\"`" + `;Items []itemRequest}
 requestBody:=struct{Data []orderRequest}{}
 var statements []string
 for n:=0;n<parentCount;n++{
  id:=1000+n;order:=orderRequest{Id:id};label:="measured";if wideProfile{label=fmt.Sprint("order",id)}
  statements=append(statements,fmt.Sprintf("INSERT INTO ORDERS(ID,KIND_ID,NAME,START,END) VALUES(%d,7,'%s','2026-09-01T00:00:00Z','2026-09-30T00:00:00Z')",id,label))
  if wideProfile{var assignments []string;for k:=0;k<16;k++{assignments=append(assignments,fmt.Sprintf("EXTRA%d='o%d-%d'",k,id,k))};statements=append(statements,fmt.Sprintf("UPDATE ORDERS SET %s WHERE ID=%d",strings.Join(assignments,","),id))}
  for j:=0;j<10;j++{itemID:=id*100+j;item:=itemRequest{Id:itemID};label="measured";if wideProfile{label=fmt.Sprint("item",itemID)}
   statements=append(statements,fmt.Sprintf("INSERT INTO ITEMS VALUES(%d,%d,'%s')",itemID,id,label))
   for k:=0;k<5;k++{detailID:=itemID*100+k;item.Details=append(item.Details,detailRequest{Id:detailID});label="measured";if wideProfile{label=fmt.Sprint("detail",detailID)};statements=append(statements,fmt.Sprintf("INSERT INTO DETAILS VALUES(%d,%d,'%s')",detailID,itemID,label))}
   order.Items=append(order.Items,item)
  };requestBody.Data=append(requestBody.Data,order)
 }
 if err=db.ExecStatements(ctx,statements...);err!=nil{t.Fatal(err)}
 encoded,err:=json.Marshal(requestBody);if err!=nil{t.Fatal(err)}
 if _,err=invoke(string(encoded));err==nil||!strings.Contains(err.Error(),"cost capture stop"){t.Fatal(err)}
 input,readContext:=measuredInput,measuredContext
 if len(input.CurrentOrders)!=parentCount||len(input.CurrentItems)!=parentCount*10||len(input.CurrentDetails)!=parentCount*50{t.Fatalf("unexpected scoped row counts %d/%d/%d",len(input.CurrentOrders),len(input.CurrentItems),len(input.CurrentDetails))}
 reads,err:=input.ReadIndexes(readContext);if err!=nil{t.Fatal(err)}
 definition:=NewOrdersHandler()
 t.Run("evidence_only",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{if err:=benchmarkEvidence(readContext,input);err!=nil{b.Fatal(err)}}})
 t.Run("clone_only",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{value,err:=benchmarkClone(input);if err!=nil{b.Fatal(err)};preparedIndexSink=value}})
 t.Run("required_key_link_indexes_only",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{if err:=benchmarkRequired(reads);err!=nil{b.Fatal(err)}}})
 t.Run("optional_business_groups_only",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{benchmarkOptional(reads)}})
 t.Run("manual_required_full",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{if err:=benchmarkEvidence(readContext,input);err!=nil{b.Fatal(err)};value,err:=benchmarkClone(input);if err!=nil{b.Fatal(err)};if err=benchmarkRequired(value);err!=nil{b.Fatal(err)};preparedIndexSink=value}})
 t.Run("default_preparation",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{if err:=input.PrepareReadIndexes(readContext);err!=nil{b.Fatal(err)}}})
 t.Run("graph_and_indexes_capture",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{program,err:=definition.Capture(readContext,input);if err!=nil{b.Fatal(err)};capturedProgramSink=program}})
 t.Run("cached_access",func(b *testing.B){b.ReportAllocs();b.ResetTimer();for n:=0;n<b.N;n++{indexes,err:=input.ReadIndexes(readContext);if err!=nil{b.Fatal(err)};preparedIndexSink=indexes}})
}
func BenchmarkGeneratedPreparation(b *testing.B){for _,size:=range []int{10,100}{b.Run(fmt.Sprintf("parents_%d_items_%d_details_%d",size,size*10,size*50),func(b *testing.B){benchmarkPreparedGraph(b,size)})}}

func benchmarkEvidence(ctx context.Context,input *OrdersInput)error{
 metadata,ok:=sdk.ReadMetadataFromContext(ctx);if !ok{return fmt.Errorf("metadata missing")}
 orders:=[]string{"Id","KindId","Name","Start","End"};if wideProfile{for n:=0;n<16;n++{orders=append(orders,fmt.Sprint("Extra",n))}}
 for _,read:=range []struct{path string;count int;fields []string}{
 {"CurrentOrders",len(input.CurrentOrders),orders},
 {"CurrentItems",len(input.CurrentItems),[]string{"Id","OrderId","Name"}},
 {"CurrentDetails",len(input.CurrentDetails),[]string{"Id","ItemId","Note"}},
 {"CurrentKinds",len(input.CurrentKinds),[]string{"Id","Name"}},
 }{
  projection,err:=metadata.Projection(read.path);if err!=nil{return err};if (shape.Runtime{}).IsNil(projection){return fmt.Errorf("missing projection")}
  for n:=0;n<read.count;n++{fields,err:=projection.Fields(n);if err!=nil{return err};if (shape.Runtime{}).IsNil(fields){return fmt.Errorf("missing field evidence")};for _,field:=range read.fields{if !fields.Has(field){return fmt.Errorf("unloaded %s.%s",read.path,field)}}}
 }
 return nil
}
func benchmarkClone(input *OrdersInput)(*OrdersHandlerReadIndexes,error){
 runtime:=shape.Runtime{};options:=shape.CloneOptions{}
 orders,err:=runtime.CloneValue(input.CurrentOrders,options);if err!=nil{return nil,err}
 items,err:=runtime.CloneValue(input.CurrentItems,options);if err!=nil{return nil,err}
 details,err:=runtime.CloneValue(input.CurrentDetails,options);if err!=nil{return nil,err}
 kinds,err:=runtime.CloneValue(input.CurrentKinds,options);if err!=nil{return nil,err}
 return &OrdersHandlerReadIndexes{CurrentOrders:orders.([]*CurrentOrdersView),CurrentItems:items.([]*CurrentItemsView),CurrentDetails:details.([]*CurrentDetailsView),CurrentKinds:kinds.([]*CurrentKindsView)},nil
}
func benchmarkRequired(reads *OrdersHandlerReadIndexes)error{
 var err error
 if reads.CurrentOrdersById,err=reads.CurrentOrders.IndexById();err!=nil{return err}
 if reads.CurrentItemsById,err=reads.CurrentItems.IndexById();err!=nil{return err}
 if reads.CurrentDetailsById,err=reads.CurrentDetails.IndexById();err!=nil{return err}
 if reads.CurrentKindsById,err=reads.CurrentKinds.IndexById();err!=nil{return err}
 reads.CurrentOrdersGroupedById=reads.CurrentOrders.GroupById()
 reads.CurrentOrdersGroupedByKindId=reads.CurrentOrders.GroupByKindId()
 reads.CurrentItemsGroupedById=reads.CurrentItems.GroupById()
 reads.CurrentItemsGroupedByOrderId=reads.CurrentItems.GroupByOrderId()
 reads.CurrentDetailsGroupedByItemId=reads.CurrentDetails.GroupByItemId()
 reads.CurrentKindsGroupedById=reads.CurrentKinds.GroupById()
 return nil
}
func benchmarkOptional(reads *OrdersHandlerReadIndexes){
 groupSink=reads.CurrentOrders.GroupByName();groupSink=reads.CurrentOrders.GroupByStart();groupSink=reads.CurrentOrders.GroupByEnd()
 groupSink=reads.CurrentItems.GroupByName();groupSink=reads.CurrentDetails.GroupByNote();groupSink=reads.CurrentKinds.GroupByName()
`
	if wide {
		for n := 0; n < 16; n++ {
			result += fmt.Sprintf("groupSink=reads.CurrentOrders.GroupByExtra%d()\n", n)
		}
	}
	return result + fmt.Sprintf("}\nconst wideProfile=%v\n", wide)
}
