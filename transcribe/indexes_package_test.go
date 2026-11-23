package transcribe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

func TestPatchIndexesPackage(t *testing.T) {
	const module = "github.com/viant/datly/genfixture"
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(t.Context(), genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	settings := "#import('orderidx','" + module + "/index')\n" +
		"#setting($_ = $indexes_package('orderidx'))\n" +
		"#setting($_ = $dest('nested/model/rows.go'))\n" +
		"#setting($_ = $support_dest('indexes','lookup.go'))\n"
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Orders", Scope: module + "/source", Text: settings + genpatch.LifecycleDQL, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	generate := func() error {
		_, err := (Generator{Operation: "patch"}).Generate(t.Context(), request)
		return err
	}
	modelFile := filepath.Join(root, "nested", "model", "rows.go")
	if _, err := os.Stat(filepath.Dir(modelFile)); !os.IsNotExist(err) {
		t.Fatalf("expected a fresh model destination, got %v", err)
	}
	if err := generate(); err != nil {
		t.Fatal(err)
	}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	genpatch.ObserveHooks(t, filepath.Join(root, "api/orders"))
	modelBefore := read("nested/model/rows.go")
	before := read("index/lookup.go")
	if !strings.Contains(before, "GroupById()") || strings.Contains(before, "PrepareReadIndexes(") {
		t.Fatal("unexpected index helper ownership")
	}
	owner := read("api/orders/lookup.go")
	if !strings.Contains(owner, "PrepareReadIndexes(") || !strings.Contains(owner, module+"/index") {
		t.Fatal("input lost its read-index API")
	}
	if err := generate(); err != nil {
		t.Fatal(err)
	}
	if modelBefore != read("nested/model/rows.go") || before != read("index/lookup.go") || owner != read("api/orders/lookup.go") {
		t.Fatal("split index regeneration changed output")
	}
	writeSourceFile(t, root, "api/orders/index_api_test.go", `package orders_test

import (
	"testing"
	orders "github.com/viant/datly/genfixture/api/orders"
)

func TestIndexKeyFromOriginalPackage(t *testing.T) {
	if _, ok := orders.OrdersHandlerCurrentKindsIndexByIdKey(nil); ok {
		t.Fatal("nil row produced a key")
	}
}
`)
	genpatch.Run(t, root, filepath.Join(root, "api/orders"), genpatch.RuntimeSource)
	for _, tc := range []struct{ name, text, want string }{
		{"unknown alias", strings.Replace(request.Source.Text, "indexes_package('orderidx')", "indexes_package('missing')", 1), "unknown import alias"},
		{"foreign destination", strings.Replace(request.Source.Text, module+"/index", "example.org/foreign/index", 1), "outside project module"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := request.Source.Text
			request.Source.Text = tc.text
			defer func() { request.Source.Text = original }()
			if err := generate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %s, got %v", tc.want, err)
			}
			if before != read("index/lookup.go") || owner != read("api/orders/lookup.go") {
				t.Fatal("failed generation changed published files")
			}
		})
	}
	// A read row owned by the input package must retain its typed API there.
	// Start with a fresh destination so previous model files cannot supply it.
	root = t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	request.Destination = root
	request.Source.Text = strings.Replace(request.Source.Text, "#setting($_ = $dest('nested/model/rows.go'))", "", 1)
	if err := generate(); err != nil {
		t.Fatal(err)
	}
	local, generic := read("api/orders/lookup.go"), read("index/lookup.go")
	if !strings.Contains(local, "func OrdersHandlerCurrentKindsIndexByIdKey(") ||
		strings.Contains(generic, module+"/api/orders") || !strings.Contains(generic, "~map[") {
		t.Fatal("owner-local index lost typed API or introduced a reverse import")
	}
	if err := generate(); err != nil {
		t.Fatal(err)
	}
	if read("api/orders/lookup.go") != local || read("index/lookup.go") != generic {
		t.Fatal("owner-local index regeneration drifted")
	}

}

func TestIndexesPackageCLI(t *testing.T) {
	const module = "github.com/viant/datly/indexfixture"
	root := t.TempDir()
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(t.Context(), genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	dql := "#import('orderidx','" + module + "/index')\n" +
		"#import('contract','" + module + "/contract')\n" +
		"#setting($_ = $indexes_package('orderidx'))\n" +
		"#setting($_ = $input_type('contract.OrdersInput'))\n" +
		"#setting($_ = $dest('model/rows.go'))\n" + genpatch.DQL
	writeSourceFile(t, root, "source/Orders.dql", dql)
	for pass := range 2 {
		runDatlyTranscribeCLI(t, root, module, filepath.Join(db.TempDir, "test.db"), true)
		if pass == 0 {
			// A real application caller remains present during CLI rediscovery.
			writeSourceFile(t, root, "contract/caller.go", `package contract
import ("context"; orderidx "github.com/viant/datly/indexfixture/index")
func indexedKinds(ctx context.Context, input *OrdersInput) (orderidx.OrdersHandlerCurrentKindsSlice, error) {
	indexes, err := input.ReadIndexes(ctx)
	if err != nil {return nil, err}
	return indexes.CurrentKinds, nil
}`)
		}
	}
}
