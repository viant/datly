package spec

import "testing"

func TestComponentCloneIsolatesParamBindingFlags(t *testing.T) {
	required := true
	cacheable := false
	value := "7"
	source := &Component{Parameters: []*Parameter{{Name: "ID", Required: &required, Cacheable: &cacheable, Value: &value}}}
	cloned := source.Clone()
	*cloned.Parameters[0].Required = false
	*cloned.Parameters[0].Cacheable = true
	*cloned.Parameters[0].Value = "8"
	if !*source.Parameters[0].Required || *source.Parameters[0].Cacheable || *source.Parameters[0].Value != "7" {
		t.Fatalf("clone mutated source flags: source=%+v clone=%+v", source.Parameters[0], cloned.Parameters[0])
	}
}

func TestComponentCloneIsolatesIndependentViews(t *testing.T) {
	groupable := false
	child := &View{Name: "Detail", TypeName: "DetailRow", Dest: "details.go", Source: &ViewSource{SQL: "SELECT 1"}}
	source := &Component{
		RootView: &View{Name: "Root"},
		Views: []*View{{Name: "Audit", Source: &ViewSource{SQL: "SELECT * FROM audit", URI: "queries/audit.sql"}, Columns: []*Column{{Name: "ID", Groupable: &groupable}}, Relations: []*Relation{{
			Name: "Detail", View: child,
		}}}},
	}
	cloned := source.Clone()
	cloned.Views[0].Source.SQL = "changed"
	cloned.Views[0].Source.URI = "changed.sql"
	*cloned.Views[0].Columns[0].Groupable = true
	cloned.Views[0].Relations[0].View.Source.SQL = "changed child"
	if source.Views[0].Source.SQL != "SELECT * FROM audit" || source.Views[0].Source.URI != "queries/audit.sql" || child.Source.SQL != "SELECT 1" ||
		source.Views[0].Columns[0].Groupable == nil || *source.Views[0].Columns[0].Groupable ||
		cloned.Views[0].Relations[0].View.TypeName != "DetailRow" || cloned.Views[0].Relations[0].View.Dest != "details.go" {
		t.Fatalf("clone mutated independent views: %+v", source.Views)
	}
}

func TestComponentCloneIsolatesRouteMCPExposures(t *testing.T) {
	source := &Component{Routes: []*Route{{
		Method: "GET", Path: "/orders",
		MCP: []*MCPExposure{{Kind: MCPExposureTool, Name: "orders.list", Description: "List orders"}},
	}}}
	cloned := source.Clone()
	cloned.Routes[0].MCP[0].Name = "changed"
	cloned.Routes[0].MCP = append(cloned.Routes[0].MCP, &MCPExposure{Kind: MCPExposureResource, Name: "orders"})
	if len(source.Routes[0].MCP) != 1 || source.Routes[0].MCP[0].Name != "orders.list" {
		t.Fatalf("clone mutated source route exposure: source=%+v clone=%+v", source.Routes[0], cloned.Routes[0])
	}
}
