package transform

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCompiledPathsSelectAndAssign(t *testing.T) {
	path, err := CompilePointer("/items/0/~1id")
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := path.Select(map[string]any{"items": []any{map[string]any{"/id": json.Number("9007199254740993")}}})
	if err != nil || !found || value != json.Number("9007199254740993") {
		t.Fatalf("large ID value=%#v found=%v err=%v", value, found, err)
	}
	object := map[string]any{}
	writer, err := CompilePointer("/profile/name")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Assign(object, "Ada"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(object, map[string]any{"profile": map[string]any{"name": "Ada"}}) {
		t.Fatalf("assigned object: %#v", object)
	}
	if err := writer.Assign(object, "Eve"); err == nil {
		t.Fatal("duplicate assignment accepted")
	}
	for _, invalid := range []string{"/items/-1/name", "/items/+1/name", "/items/01/name", "/items/999999999999999999999999999/name"} {
		path, err := CompilePointer(invalid)
		if err != nil {
			continue // overflow may be rejected during compilation
		}
		if _, _, err := path.Select(map[string]any{"items": []any{"x"}}); err == nil {
			t.Fatalf("invalid array index %q accepted", invalid)
		}
	}
	key, _ := CompilePointer("/01")
	if value, found, err := key.Select(map[string]any{"01": "object key"}); err != nil || !found || value != "object key" {
		t.Fatalf("numeric object key value=%#v found=%v err=%v", value, found, err)
	}
	numericKey, _ := CompilePointer("/items/0/name")
	numericObject := map[string]any{}
	if err := numericKey.Assign(numericObject, "zero"); err != nil {
		t.Fatal(err)
	}
	if value, found, err := numericKey.Select(numericObject); err != nil || !found || value != "zero" {
		t.Fatalf("numeric object key after assignment value=%#v found=%v err=%v", value, found, err)
	}
}

func TestStrictSelectorAndStructSource(t *testing.T) {
	type item struct{ ID int64 }
	type source struct{ Items []item }
	path, err := CompileSelector("Items[0].ID")
	if err != nil {
		t.Fatal(err)
	}
	if err := path.ValidateType(reflect.TypeFor[source]()); err != nil {
		t.Fatal(err)
	}
	value, found, err := path.Select(&source{Items: []item{{ID: 9007199254740993}}})
	if err != nil || !found || value != int64(9007199254740993) {
		t.Fatalf("struct path value=%#v found=%v err=%v", value, found, err)
	}
	for _, raw := range []string{"", ".Items", "Items.", "Items..ID", "Items.[0]", "Items[-1]", "Items[01]", "Items[x]", "Items[0", "Items[0]ID"} {
		if _, err := CompileSelector(raw); err == nil {
			t.Fatalf("malformed selector %q accepted", raw)
		}
	}
	for _, raw := range []string{"items", "/bad~2escape", "items/0"} {
		if _, err := CompilePointer(raw); err == nil {
			t.Fatalf("malformed pointer %q accepted", raw)
		}
	}
}

func TestCompiledSourceHandlesNilPromotedPointerAndCancellation(t *testing.T) {
	type embedded struct{ Name string }
	type source struct{ *embedded }
	type output struct{ Name string }
	path, err := CompileSelector("Name")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := CompileFor(reflect.TypeFor[source](), reflect.TypeFor[output](), []Mapping{{From: path, To: "Name"}})
	if err != nil {
		t.Fatal(err)
	}
	var result output
	if err := plan.Apply(context.Background(), source{}, &result); err != nil {
		t.Fatalf("nil promoted source panicked or failed: %v", err)
	}
	if result.Name != "" {
		t.Fatalf("absent promoted field set Name=%q", result.Name)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	withCodec, err := Compile(reflect.TypeFor[output](), []Mapping{{From: path, To: "Name", Transform: func(context.Context, any) (any, error) {
		called = true
		return "wrong", nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := withCodec.Apply(ctx, source{embedded: &embedded{Name: "Ada"}}, &result); err != context.Canceled || called {
		t.Fatalf("cancellation error=%v nested codec called=%v", err, called)
	}
}

func TestPlanUsesBindlyForStrictNestedTypedAssignment(t *testing.T) {
	type contextValue struct {
		ID      int64            `json:"id"`
		Allowed map[string][]int `json:"allowed"`
	}
	type output struct{ Context *contextValue }
	from, _ := CompilePointer("/user")
	plan, err := Compile(reflect.TypeFor[output](), []Mapping{{From: from, To: "Context", Required: true}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		source map[string]any
		valid  bool
	}{
		{"precise", map[string]any{"user": map[string]any{"id": json.Number("9007199254740993"), "allowed": map[string]any{"project": []any{json.Number("101")}}}}, true},
		{"missing", map[string]any{}, false},
		{"null", map[string]any{"user": nil}, false},
		{"wrong scalar", map[string]any{"user": map[string]any{"id": "7"}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var result output
			err := plan.Apply(context.Background(), test.source, &result)
			if !test.valid {
				if err == nil {
					t.Fatalf("invalid source accepted: %#v", result)
				}
				return
			}
			if err != nil || result.Context == nil || result.Context.ID != 9007199254740993 || !reflect.DeepEqual(result.Context.Allowed["project"], []int{101}) {
				t.Fatalf("context=%#v err=%v", result.Context, err)
			}
		})
	}
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			var result output
			if err := plan.Apply(context.Background(), map[string]any{"user": map[string]any{"id": json.Number("7")}}, &result); err != nil || result.Context == nil || result.Context.ID != 7 {
				t.Errorf("concurrent result=%#v err=%v", result, err)
			}
		}()
	}
	wait.Wait()
}

func TestOptionalMappingLeavesParentNil(t *testing.T) {
	type nested struct{ Name string }
	type output struct{ Context *nested }
	from, _ := CompilePointer("/missing")
	plan, err := Compile(reflect.TypeFor[output](), []Mapping{{From: from, To: "Context.Name"}})
	if err != nil {
		t.Fatal(err)
	}
	var result output
	if err := plan.Apply(context.Background(), map[string]any{}, &result); err != nil || result.Context != nil {
		t.Fatalf("optional output=%#v err=%v", result, err)
	}
	if _, err := CompileFor(reflect.TypeFor[struct{ ID int }](), reflect.TypeFor[output](), []Mapping{{From: from, To: "Context.Name"}}); err == nil || !strings.Contains(err.Error(), "input path") {
		t.Fatalf("unknown typed source accepted: %v", err)
	}
}

func TestCompileDetachesCallerMappings(t *testing.T) {
	type source struct{ Name string }
	type output struct{ Name string }
	path, _ := CompileSelector("Name")
	mappings := []Mapping{{From: path, To: "Name", Required: true}}
	plan, err := CompileFor(reflect.TypeFor[source](), reflect.TypeFor[output](), mappings)
	if err != nil {
		t.Fatal(err)
	}
	mappings[0] = Mapping{To: "Wrong"}
	var result output
	if err := plan.Apply(context.Background(), source{Name: "Ada"}, &result); err != nil || result.Name != "Ada" {
		t.Fatalf("caller mutation changed plan: result=%#v err=%v", result, err)
	}
}
