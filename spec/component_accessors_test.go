package spec

import "testing"

func TestComponentAccessors(t *testing.T) {
	rootView := &View{
		Source: &ViewSource{SQL: " SELECT 1 "},
	}
	component := &Component{
		Settings: &Settings{
			Cache: &CacheSettings{
				Warmup: &CacheWarmupSettings{IndexColumn: "vendor_id"},
			},
		},
		RootView: rootView,
	}

	if warmup := component.CacheWarmup(); warmup == nil || warmup.IndexColumn != "vendor_id" {
		t.Fatalf("unexpected warmup accessor: %+v", warmup)
	}
	if source := component.RootSource(); source == nil || source.SQL != " SELECT 1 " {
		t.Fatalf("unexpected root source accessor: %+v", source)
	}
	if sql := component.RootSQL(); sql != "SELECT 1" {
		t.Fatalf("unexpected root sql accessor: %q", sql)
	}
}
