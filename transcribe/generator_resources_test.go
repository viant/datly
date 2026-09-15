package transcribe

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestGeneratorCompiledResourceScope(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	resources := resource.New()
	if err := resources.Register("restricted", fstest.MapFS{
		"orders.sql": &fstest.MapFile{Data: []byte("SELECT * FROM ORDERS WHERE NAME = 'permitted'")},
		"kinds.sql":  &fstest.MapFile{Data: []byte("SELECT * FROM ORDER_KINDS WHERE NAME = 'standard'")},
	}); err != nil {
		t.Fatal(err)
	}
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "Orders", Scope: "resource-proof", Text: genpatch.LifecycleDQL, Resources: resources, Types: typecatalog.NewCatalog(), Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})})
	if err != nil {
		t.Fatal(err)
	}
	compiled.Component.RootView.Source.SQL = ""
	compiled.Component.RootView.Source.URI = "restricted:orders.sql"
	for _, relation := range compiled.Component.RootView.Relations {
		if relation.View.Auxiliary {
			relation.View.Source.SQL = ""
			relation.View.Source.URI = "restricted:kinds.sql"
		}
	}
	request := GenerationRequest{Compiled: compiled, Destination: root}
	got, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, predicate := range []string{"NAME = 'permitted'", "NAME = 'standard'"} {
		found := false
		for _, file := range got.Result.Files {
			if strings.HasSuffix(file.Path, ".sql") && strings.Contains(file.Content, predicate) {
				found = true
			}
		}
		if !found {
			t.Fatal("resource predicate was lost", predicate)
		}
	}
	if compiled.Component.RootView.Source.SQL != "" || compiled.Component.RootView.Source.URI != "restricted:orders.sql" {
		t.Fatal("compiled source mutated")
	}
	directory := filepath.Join(root, strings.TrimPrefix(got.Package.PkgPath, "github.com/viant/datly/genfixture/"))
	genpatch.ObserveHooks(t, directory)
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	source := genpatch.RuntimeSource
	source = strings.Replace(source, "var lookupRead bool", "var lookupRead bool\nvar deniedCurrent bool\nvar restrictedLookupCount = -1", 1)
	source = strings.Replace(source, "for _,order:=range input.Orders{if order!=nil{", `for _,order:=range input.Orders{if order!=nil{
  if order.Id!=nil && *order.Id==2{deniedCurrent=len(input.CurrentOrders)!=0}
  if order.KindId!=nil && *order.KindId==1{restrictedLookupCount=len(input.CurrentKinds);return fmt.Errorf("lookup outside authored source")}`, 1)
	source = strings.Replace(source, "var foreignKeys int;", `if err:=db.ExecStatements(ctx,"UPDATE ORDERS SET NAME='permitted' WHERE ID=1", "INSERT INTO ORDERS VALUES(2,7,'blocked','2026-09-01T00:00:00Z','2026-09-30T00:00:00Z')");err!=nil{t.Fatal(err)}
 var foreignKeys int;`, 1)
	source = strings.Replace(source, " actual,err:=invoke(", ` _,denied:=invoke(`+"`"+`{"Data":[{"id":2,"kindId":7,"name":"unauthorized change","start":"2026-09-02T00:00:00Z","end":"2026-09-30T00:00:00Z"}]}`+"`"+`)
 if denied==nil||deniedCurrent{t.Fatalf("restricted Current admitted denied parent: %v",denied)}
 var protected string;if err:=db.DB.QueryRow("SELECT NAME FROM ORDERS WHERE ID=2").Scan(&protected);err!=nil||protected!="blocked"{t.Fatalf("denied row changed: %s %v",protected,err)}
 _,denied=invoke(`+"`"+`{"Data":[{"id":1,"kindId":1}]}`+"`"+`)
 if denied==nil||restrictedLookupCount!=0{t.Fatalf("auxiliary source restriction lost: count=%d err=%v",restrictedLookupCount,denied)}
 actual,err:=invoke(`, 1)
	genpatch.Run(t, root, directory, source)
}
