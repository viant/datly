package writer

import (
	"context"
	"database/sql"
	"errors"
	"github.com/viant/bindly"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"reflect"
	"testing"
)

type prebindingDatabaseKey struct{}
type prebindingProbeInput struct {
	Rows     []*struct{}
	Observed bool
}

var errPrebindingStop = errors.New("stop after transaction observation")

func (in *prebindingProbeInput) Init(ctx context.Context) error {
	db := ctx.Value(prebindingDatabaseKey{}).(*sql.DB)
	tx, err := dexec.InvocationTransaction(ctx, db)
	if err != nil {
		return err
	}
	if tx == nil {
		return errors.New("input initialization had no writer transaction")
	}
	in.Observed = true
	// A diagnostic write in the fixture proves that the same invocation owns
	// rollback even when input initialization stops before mutation capture.
	if _, err := tx.ExecContext(ctx, "INSERT INTO probe(id) VALUES(1)"); err != nil {
		return err
	}
	return errPrebindingStop
}

func TestWriterTransactionAvailableBeforeInputInitSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, "CREATE TABLE probe(id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	typeOf := reflect.TypeFor[prebindingProbeInput]()
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(typeOf)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "PATCH", Path: "/probe"}
	contract, err := registry.NewInputContract(typeOf, projection, registry.RouteInput{Route: ref, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	route, ok := contract.ForRoute(ref)
	if !ok {
		t.Fatal("missing route")
	}
	input := &prebindingProbeInput{}
	handler := &Handler{inputType: typeOf, outputType: reflect.TypeFor[struct{}](), metadata: &Metadata{Root: &Record{}}, preBindingTransaction: true}
	ctx = context.WithValue(ctx, prebindingDatabaseKey{}, h.DB)
	_, err = engine.New().Execute(ctx, engine.Request{Input: route, BoundInput: input, DataSource: dml.Source{DB: h.DB}, Handler: handler})
	if !errors.Is(err, errPrebindingStop) || !input.Observed {
		t.Fatalf("observed=%v error=%v", input.Observed, err)
	}
	var count int
	if err := h.DB.QueryRow("SELECT COUNT(*) FROM probe").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("input failure did not roll back the invocation transaction")
	}
}
