package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
	"path/filepath"
	"strings"
	"testing"
)

func TestCorrectionCacheEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	got, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: "example.com/generated/orders", Text: genpatch.LifecycleDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	genpatch.ObserveResolvedHooks(t, directory)
	// Entity-sync fixtures own race instrumentation; this generated module
	// validates correction/cache semantics without another full race build.
	genpatch.Run(t, root, directory, correctionCacheRuntime(false), "-run", "TestIndependentCacheAndEvidence", "-v")
}
func correctionCacheRuntime(deep bool) string {
	source := strings.Replace(genpatch.ResolvedIdentityRuntime(deep), ` "context"`, ` "context";sdk "github.com/viant/xdatly/handler"`, 1)
	return source + `
type independentFields struct{missing bool}
func(f independentFields)Has(name string)bool{return !(f.missing&&name=="Name")}
type independentProjection struct{missing bool}
func(independentProjection)RootHolder()string{return ""}
func(independentProjection)DirectOutput()bool{return true}
func(p independentProjection)Fields(int,...sdk.ReadStep)(sdk.FieldSet,error){return independentFields{p.missing},nil}
type independentMetadata struct{missing bool}
func(m independentMetadata)Projection(path string)(sdk.ReadProjection,error){return independentProjection{m.missing&&path=="CurrentKinds"},nil}
func independentSetKinds(input *OrdersInput,ids ...int){
 field:=reflect.ValueOf(input).Elem().FieldByName("CurrentKinds")
 rows:=reflect.MakeSlice(field.Type(),0,len(ids))
 for _,id:=range ids{
  row:=reflect.New(field.Type().Elem().Elem())
  idField:=row.Elem().FieldByName("Id");v:=reflect.New(idField.Type().Elem());v.Elem().SetInt(int64(id));idField.Set(v)
  nameField:=row.Elem().FieldByName("Name");n:=reflect.New(nameField.Type().Elem());n.Elem().SetString("standard");nameField.Set(n)
  rows=reflect.Append(rows,row)
 }
 field.Set(rows)
}
func TestIndependentCacheAndEvidence(t *testing.T){
 ctx:=sdk.WithReadMetadata(context.Background(),independentMetadata{})
 input:=&OrdersInput{}
 independentSetKinds(input,7)
 if err:=input.PrepareReadIndexes(ctx);err!=nil{t.Fatal(err)}
 first,err:=input.ReadIndexes(ctx);if err!=nil||!first.CurrentKindsById.Has(7){t.Fatal("first capture",err)}
 *first.CurrentKindsById[7].Id=777
 if *input.CurrentKinds[0].Id!=7{t.Fatal("helper mutated read input")}
 independentSetKinds(input,8)
 if err=input.PrepareReadIndexes(ctx);err!=nil{t.Fatal(err)}
 second,err:=input.ReadIndexes(ctx);if err!=nil||first==second||!second.CurrentKindsById.Has(8)||second.CurrentKindsById.Has(7){t.Fatal("stale invocation cache",err)}
 reduced:=sdk.WithReadMetadata(context.Background(),independentMetadata{missing:true})
 if err=input.PrepareReadIndexes(reduced);err==nil||!strings.Contains(err.Error(),"application index field was not loaded: CurrentKinds.Name"){t.Fatalf("unused auxiliary reduced projection = %v",err)}
 if input._ordersHandlerReadIndexes!=nil{t.Fatal("failed preparation left stale cache")}
 independentSetKinds(input,8,8)
 if err=input.PrepareReadIndexes(ctx);err==nil||!strings.Contains(err.Error(),"ambiguous application index"){t.Fatalf("duplicate auxiliary primary keys = %v",err)}
 t.Log("same-input Capture refresh, detached rows, reduced unused auxiliary evidence failure, cache clearing, and duplicate primary-key rejection verified")
}
`
}
