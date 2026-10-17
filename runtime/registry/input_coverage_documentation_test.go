package registry

import (
	"context"
	"github.com/viant/bindly/resource"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
	"testing"
	"testing/fstest"
)

func TestScopedCoverageKeepsOwnerLocalDocumentation(t *testing.T) {
	snapshot := func(text string) *docs.Snapshot {
		store := resource.New()
		if err := store.Register("doc", fstest.MapFS{"params.yaml": &fstest.MapFile{Data: []byte("parameters:\n  Value: '" + text + "'\n  Value$example: '" + text + " example'\n")}}); err != nil {
			t.Fatal(err)
		}
		value, err := (docs.Loader{Resources: store}).Load(context.Background(), xdocs.Source{DocURL: "doc:params.yaml"})
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	for _, tc := range []struct {
		name          string
		parent, child *docs.Snapshot
		authored      bool
	}{
		{"parent only", snapshot("parent"), nil, false},
		{"child only", nil, snapshot("child"), false},
		{"different snapshots", snapshot("parent"), snapshot("child"), false},
		{"different authored", nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pb, cb := coverageValue(true), coverageValue(false)
			if tc.authored {
				pb.Extension = &spec.Parameter{Description: "parent", Example: "parent example"}
				cb.Extension = &spec.Parameter{Description: "child", Example: "child example"}
			}
			parent := coverageContract(t, "/root", pb, coverageEdge("First", "/child", "lazy")).WithDocumentation(tc.parent)
			child := coverageContract(t, "/child", cb).WithDocumentation(tc.child)
			catalog, err := NewInputCatalog([]*RegisteredComponent{
				{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "root"}, Routes: []*spec.Route{{Method: "GET", Path: "/root"}}}, Input: parent},
				{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "child"}, Routes: []*spec.Route{{Method: "GET", Path: "/child"}}}, Input: child},
			})
			if err != nil {
				t.Fatal(err)
			}
			fields, err := coverageFields(t, catalog)
			if err != nil {
				t.Fatal(err)
			}
			if len(fields) != 1 || fields[0].Origin().Path != "/root" || fields[0].Documentation() != tc.parent {
				t.Fatal("authority documentation or identity changed")
			}
			if tc.authored && (pb.Extension.(*spec.Parameter).Description != "parent" || cb.Extension.(*spec.Parameter).Description != "child" || fields[0].Binding().Extension.(*spec.Parameter).Example != "parent example") {
				t.Fatal("authored documentation mutated or replaced")
			}
			route, _ := child.ForRoute(spec.RouteRef{Method: "GET", Path: "/child"})
			if route.Fields()[0].Documentation() != tc.child {
				t.Fatal("child snapshot mutated")
			}
		})
	}
}
