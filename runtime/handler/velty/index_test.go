package velty

import (
	"context"
	"strings"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
)

func TestIndexBuildsAndFindsOrderedCompoundKeys(t *testing.T) {
	type row struct {
		TenantID *string
		ID       *uint64
	}
	type input struct {
		Events []*row
	}
	tenant := "acme"
	id := uint64(7)
	index := newIndex()
	if _, err := index.Build("events", []*row{{TenantID: &tenant, ID: &id}, nil, {TenantID: nil, ID: &id}}, &input{}, "Events",
		"TenantID", "TenantID", "ID", "ID"); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	for _, testCase := range []struct {
		name   string
		record any
		want   bool
	}{
		{name: "exact", record: &row{TenantID: &tenant, ID: &id}, want: true},
		{name: "value record", record: row{TenantID: &tenant, ID: &id}, want: true},
		{name: "missing", record: &row{TenantID: &tenant, ID: pointerTo(uint64(8))}},
		{name: "nil part", record: &row{ID: &id}},
		{name: "nil record"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			matched, err := index.Has("events", testCase.record)
			if err != nil || matched != testCase.want {
				t.Fatalf("Has() = (%v, %v), want (%v, nil)", matched, err, testCase.want)
			}
		})
	}
}

func TestIndexRejectsDuplicateCompoundKey(t *testing.T) {
	type row struct {
		TenantID string
		ID       int64
	}
	type input struct {
		Events []row
	}
	index := newIndex()
	_, err := index.Build("events", []row{{TenantID: "acme", ID: 7}, {TenantID: "acme", ID: 7}}, input{}, "Events",
		"TenantID", "TenantID", "ID", "ID")
	if err == nil || !strings.Contains(err.Error(), "duplicate compound key") {
		t.Fatalf("Build() error = %v, want duplicate compound key", err)
	}
}

func TestHandlerExecutesInvocationLocalCompoundIndex(t *testing.T) {
	type row struct {
		TenantID string
		ID       int64
	}
	type input struct {
		Current []*row
		Record  *row
	}
	type output struct {
		Found string
	}
	handler, err := New[input, output](Config{Template: `$index.Build("events", $Input.Current, $Input, "Record", "TenantID", "TenantID", "ID", "ID")
#if($index.Has("events", $Input.Record))
#set($Output.Found = "yes")
#end`})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	actual, err := handler.Execute(context.Background(), rhandler.Invocation{Input: &input{
		Current: []*row{{TenantID: "acme", ID: 7}}, Record: &row{TenantID: "acme", ID: 7},
	}})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result := actual.(*output); result.Found != "yes" {
		t.Fatalf("output = %+v, want Found", result)
	}
}

func TestIndexRejectsUnexpectedRecordType(t *testing.T) {
	type row struct {
		TenantID string
		ID       int64
	}
	type input struct {
		Record *row
	}
	index := newIndex()
	if _, err := index.Build("events", []row{{TenantID: "acme", ID: 7}}, input{}, "Record",
		"TenantID", "TenantID", "ID", "ID"); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if _, err := index.Has("events", &struct {
		TenantID string
		ID       int64
	}{TenantID: "acme", ID: 7}); err == nil || !strings.Contains(err.Error(), "record requires") {
		t.Fatalf("Has() error = %v, want record type error", err)
	}
}

func TestIndexResolvesNestedRecordPath(t *testing.T) {
	type item struct {
		ID       int64
		TenantID int64
	}
	type order struct {
		Items []*item
	}
	type input struct {
		Orders []*order
	}
	index := newIndex()
	if _, err := index.Build("items", []item{{ID: 3, TenantID: 7}}, input{}, "Orders/Items",
		"ID", "ID", "TenantID", "TenantID"); err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	matched, err := index.Has("items", &item{ID: 3, TenantID: 7})
	if err != nil || !matched {
		t.Fatalf("Has() = (%v, %v), want (true, nil)", matched, err)
	}
}

func pointerTo[T any](value T) *T {
	return &value
}

func TestNewContextCreatesIndexOnlyWhenReferenced(t *testing.T) {
	without, err := newContext(context.Background(), nil, programCapabilities{})
	if err != nil {
		t.Fatal(err)
	}
	if without.Index != nil {
		t.Fatalf("unused index = %#v, want nil", without.Index)
	}
	with, err := newContext(context.Background(), nil, programCapabilities{index: true})
	if err != nil {
		t.Fatal(err)
	}
	if with.Index == nil {
		t.Fatal("referenced index was not initialized")
	}
}
