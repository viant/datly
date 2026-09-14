package compiler

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestPUTExplicitCurrentBindingsPreserveUpdateOnlyPolicy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*Request, []string)
		current   [3]bool
	}{
		{name: "direct PUT does not infer available Current"},
		{name: "explicit root", configure: func(r *Request, _ []string) { r.Current = "CurrentOrders" }, current: [3]bool{true, false, false}},
		{name: "canonical root mapping", configure: func(r *Request, ids []string) {
			r.Currents = []CurrentBinding{{ViewIdentity: ids[0], Param: "CurrentOrders"}}
		}, current: [3]bool{true, false, false}},
		{name: "same root selected twice", configure: func(r *Request, ids []string) {
			r.Current = "currentorders"
			r.Currents = []CurrentBinding{{ViewIdentity: ids[0], Param: "CurrentOrders"}}
		}, current: [3]bool{true, false, false}},
		{name: "child only", configure: func(r *Request, ids []string) {
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentItems"}}
		}, current: [3]bool{false, true, false}},
		{name: "all roles", configure: func(r *Request, ids []string) {
			r.Current = "CurrentOrders"
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentItems"}, {ViewIdentity: ids[2], Param: "CurrentDetails"}}
		}, current: [3]bool{true, true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, identities := putCurrentRequest(t)
			if tc.configure != nil {
				tc.configure(&request, identities)
			}
			before, err := json.Marshal(request.Component)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := (&Compiler{}).Compile(request)
			if err != nil {
				t.Fatal(err)
			}
			if actual.Operation != plan.OperationPut {
				t.Fatal("operation changed")
			}
			records := []*plan.RecordPlan{actual.Root, actual.Root.Relations[0].Child, actual.Root.Relations[0].Child.Relations[0].Child}
			for index, record := range records {
				if (record.Current != nil) != tc.current[index] {
					t.Fatalf("role%d current=%+v", index, record.Current)
				}
				if record.Write.Existing != plan.ActionUpdate || record.Write.Missing != "" || !reflect.DeepEqual(record.Write.Allowed, []plan.Action{plan.ActionUpdate}) || record.Sequence != nil {
					t.Fatalf("role%d policy=%+v sequence=%+v", index, record.Write, record.Sequence)
				}
				if record.Current != nil && (len(record.Current.Keys) == 0 || len(record.Current.Fields) == 0) {
					t.Fatalf("role%d lost prior-read key/projection metadata", index)
				}
			}
			after, err := json.Marshal(request.Component)
			if err != nil {
				t.Fatal(err)
			}
			if string(after) != string(before) {
				t.Fatal("compiler mutated canonical input")
			}
		})
	}
}

func TestPUTCurrentBindingFailuresAndPOSTRejection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*Request, []string)
		fragment  string
	}{
		{"conflicting root", func(r *Request, ids []string) {
			r.Current = "CurrentOrders"
			r.Currents = []CurrentBinding{{ViewIdentity: ids[0], Param: "CurrentItems"}}
		}, "conflicts"},
		{"duplicate mapping", func(r *Request, ids []string) {
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentItems"}, {ViewIdentity: ids[1], Param: "CurrentItems"}}
		}, "duplicated"},
		{"unused mapping", func(r *Request, _ []string) {
			r.Currents = []CurrentBinding{{ViewIdentity: "unreachable", Param: "CurrentItems"}}
		}, "unused"},
		{"missing exact view", func(r *Request, ids []string) {
			r.ViewBindings = nil
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentItems"}}
		}, "exact canonical view binding"},
		{"unknown current", func(r *Request, _ []string) { r.Current = "Missing" }, "was not found"},
		{"root current reused in child", func(r *Request, ids []string) {
			r.Current = "CurrentOrders"
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentOrders"}}
		}, "more than one writable view"},
		{"child current reused", func(r *Request, ids []string) {
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentItems"}, {ViewIdentity: ids[2], Param: "CurrentItems"}}
		}, "more than one writable view"},
		{"one-valued current", func(r *Request, _ []string) {
			r.Current = "CurrentOrders"
			r.Component.Parameters[2].TypeExpr = "Order"
			r.Component.Parameters[2].Cardinality = string(spec.CardinalityOne)
		}, "many cardinality"},
		{"POST explicit current rejected", func(r *Request, _ []string) { r.Operation = plan.OperationPost; r.Current = "CurrentOrders" }, "PATCH and PUT"},
		{"POST mapped current rejected", func(r *Request, ids []string) {
			r.Operation = plan.OperationPost
			r.Currents = []CurrentBinding{{ViewIdentity: ids[1], Param: "CurrentItems"}}
		}, "PATCH and PUT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, ids := putCurrentRequest(t)
			tc.configure(&request, ids)
			_, err := (&Compiler{}).Compile(request)
			if err == nil || !strings.Contains(err.Error(), tc.fragment) {
				t.Fatalf("error=%v, want %q", err, tc.fragment)
			}
		})
	}
}

func TestPUTCurrentRetainsCompositeIdentityOrder(t *testing.T) {
	component := testComponent()
	for _, view := range []*spec.View{component.RootView, component.Views[0]} {
		view.Columns = append([]*spec.Column{{Name: "TENANT_ID", Source: "TENANT_ID", PrimaryKey: true, Type: spec.TypeRef{Name: "int64"}}}, view.Columns...)
	}
	actual, err := (&Compiler{}).Compile(Request{Component: component, Operation: plan.OperationPut, Current: "CurrentEvents", ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0})})
	if err != nil {
		t.Fatal(err)
	}
	keys := actual.Root.Current.Keys
	if len(keys) != 2 || keys[0].Source != "TENANT_ID" || keys[1].Source != "ID" || actual.Root.Sequence != nil {
		t.Fatalf("current=%+v", actual.Root.Current)
	}
}

func putCurrentRequest(t *testing.T) (Request, []string) {
	t.Helper()
	component, item, detail := recursiveComponent()
	var identities []string
	for _, view := range []*spec.View{component.RootView, item, detail} {
		identity, err := view.Identity()
		if err != nil {
			t.Fatal(err)
		}
		identities = append(identities, identity)
	}
	return Request{Component: component, Operation: plan.OperationPut, ViewBindings: testViewBindings(t, component, viewBindingIndex{param: 2, view: 0}, viewBindingIndex{param: 3, view: 1}, viewBindingIndex{param: 4, view: 2})}, identities
}
