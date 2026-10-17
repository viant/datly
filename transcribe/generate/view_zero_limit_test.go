package generate

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestPlanRejectsUnrepresentableRawZeroLimit(t *testing.T) {
	zero, positive := 0, 3
	for _, position := range []string{"root", "dependency", "relation"} {
		for _, tc := range []struct {
			name     string
			controls *spec.ViewControls
			selector *spec.Selector
			sql      string
			reject   bool
		}{
			{name: "raw_zero", controls: &spec.ViewControls{Limit: &zero}, reject: true},
			{name: "contradictory_zero", controls: &spec.ViewControls{Limit: &zero}, selector: &spec.Selector{NoLimit: true}, reject: true},
			{name: "positive", controls: &spec.ViewControls{Limit: &positive}},
			{name: "normalized_unlimited", selector: &spec.Selector{NoLimit: true}},
			{name: "literal_sql_zero", sql: "SELECT ID FROM records LIMIT 0"},
		} {
			t.Run(position+"/"+tc.name, func(t *testing.T) {
				v := &spec.View{Name: "Records", Source: &spec.ViewSource{Table: "records", SQL: tc.sql, Controls: tc.controls}, Selector: tc.selector}
				c := &spec.Component{Name: "Read"}
				switch position {
				case "root":
					c.RootView = v
				case "dependency":
					c.Views = []*spec.View{v}
					c.Parameters = []*spec.Parameter{{Name: "Records", Source: spec.BindSource{Kind: "view", Name: "Records"}}}
				case "relation":
					c.RootView = &spec.View{Name: "Root", Relations: []*spec.Relation{{Name: "Records", Holder: "Records", View: v}}}
				}
				_, err := New(Input{Component: c}).Plan()
				if tc.reject {
					if err == nil || !strings.Contains(err.Error(), "literal SQL-control limit zero") {
						t.Fatalf("expected zero-limit diagnostic, got %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
