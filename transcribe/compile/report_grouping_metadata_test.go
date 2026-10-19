package compile

import (
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
)

func TestBackfillReportSQLRolesWithoutDiscovery(t *testing.T) {
	sql := "SELECT c.* FROM (SELECT region AS area, LOWER(country) AS country_key, SUM(amount) AS total FROM sales GROUP BY region, LOWER(country)) c"
	for _, grouped := range []bool{true, false} {
		yes, no := true, false
		columns := []*spec.Column{
			{Name: "Area", Source: "area", Output: "area", Selector: "Area"},
			{Name: "Country", Source: "country_key", Output: "country_key", Selector: "Country"},
			{Name: "Total", Source: "total", Output: "total", Selector: "Total"},
		}
		component := &spec.Component{Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, RootView: &spec.View{Name: "Report", Groupable: &grouped, Source: &spec.ViewSource{URI: "report.sql"}, Columns: columns}}
		var unbound *resource.Store
		if err := BackfillReportMetadata(component, unbound); err != nil {
			t.Fatal(err)
		}
		if columns[0].Groupable != nil {
			t.Fatal("unbound resource acquired grouping proof")
		}
		if grouped {
			if err := BackfillReportMetadata(component, resource.New()); err == nil {
				t.Fatal("bound missing resource accepted")
			}
		}
		resources := fstest.MapFS{"report.sql": {Data: []byte(sql)}}
		if err := BackfillReportMetadata(component, resources); err != nil {
			t.Fatal(err)
		}
		for i, column := range columns {
			if grouped {
				if column.Groupable == nil || *column.Groupable != (i < 2) {
					t.Fatalf("column %d role=%v", i, column.Groupable)
				}
			} else if column.Groupable != nil {
				t.Fatal("non-groupable report changed")
			}
			if column.Output != []string{"area", "country_key", "total"}[i] {
				t.Fatal("projection order/identity changed")
			}
		}
		if component.RootView.Source.SQL != "" || component.RootView.Source.URI != "report.sql" {
			t.Fatal("source resource mutated")
		}
		// Completed metadata must not reopen a bound but now absent asset.
		if err := BackfillReportMetadata(component, resource.New()); err != nil {
			t.Fatal(err)
		}
		// Explicit pointers and tags remain authoritative even against SQL roles.
		columns[0].Groupable = &no
		columns[1].Tag = `groupable:"false"`
		columns[2].Groupable = &yes
		if err := BackfillReportMetadata(component, resources); err != nil {
			t.Fatal(err)
		}
		if *columns[0].Groupable || *columns[1].Groupable || !*columns[2].Groupable {
			t.Fatal("explicit grouping override lost")
		}
		columns[2].Tag = `groupable:"false"`
		columns[1].Tag = `groupable:"true"`
		if err := BackfillReportMetadata(component, resources); err != nil {
			t.Fatal(err)
		}
		if !*columns[1].Groupable || *columns[2].Groupable {
			t.Fatal("explicit tags did not retain precedence")
		}
	}
}

func TestBackfillReportPhysicalWildcardDoesNotInferRoles(t *testing.T) {
	yes := true
	column := &spec.Column{Name: "Region", Source: "region", Output: "region", Selector: "Region"}
	component := &spec.Component{Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, RootView: &spec.View{Name: "Report", Groupable: &yes, Source: &spec.ViewSource{SQL: "SELECT * FROM sales"}, Columns: []*spec.Column{column}}}
	if err := BackfillReportMetadata(component); err != nil {
		t.Fatal(err)
	}
	if column.Groupable != nil {
		t.Fatal("physical wildcard acquired SQL grouping role")
	}
}
