package index

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestIndexedReportMCPSelectionAndCollision(t *testing.T) {
	grouped, strict := true, false
	metadata := &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "aggregate_records", Description: "Quoted, \"text\"\nline", DescriptionPath: "docs/cube.md", CaseFormat: "lc", StrictArguments: &strict}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Records"}, RootView: &spec.View{Groupable: &grouped}, Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true, MCP: metadata}}, Routes: []*spec.Route{{Method: "GET", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "get_records"}}}}}
	entries := expandReportEntries([]*Entry{{Component: component}})
	if len(entries) != 2 || !reflect.DeepEqual(entries[1].Component.Routes[0].MCP[0], metadata) {
		t.Fatalf("entries %+v", entries)
	}
	snapshot, err := newSnapshot("selection", "fingerprint", entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.byMCP) != 2 {
		t.Fatal("indexed tools lost")
	}
	component.Routes[0].MCP[0].Name = metadata.Name
	if _, err = newSnapshot("selection", "fingerprint", expandReportEntries([]*Entry{{Component: component}})); err == nil {
		t.Fatal("collision admitted")
	}
}

func BenchmarkReportMCPIndex(b *testing.B) {
	for _, explicit := range []bool{false, true} {
		label := "legacy"
		if explicit {
			label = "explicit"
		}
		b.Run(label, func(b *testing.B) {
			grouped := true
			source := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "Records"}, RootView: &spec.View{Groupable: &grouped}, Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}}
			name := "RecordsCube"
			if explicit {
				name = "aggregate_records"
				source.Settings.Report.MCP = &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: name, Description: "Aggregate records", CaseFormat: "lc"}
			}
			entries := []*Entry{{Component: source}}
			b.Run("prepare", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := newSnapshot("selection", "fingerprint", expandReportEntries(entries)); err != nil {
						b.Fatal(err)
					}
				}
			})
			snapshot, err := newSnapshot("selection", "fingerprint", expandReportEntries(entries))
			if err != nil {
				b.Fatal(err)
			}
			b.Run("warm_indexed_lookup", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, ok := snapshot.MCP(MCPIdentity{Kind: spec.MCPExposureTool, Name: name}); !ok {
						b.Fatal("missing tool")
					}
				}
			})
		})
	}
}
