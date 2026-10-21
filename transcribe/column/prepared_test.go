package column

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestPreparedColumnsRejectDriftAndRetainSQL(t *testing.T) {
	for _, tc := range []struct {
		name, sql, table, want string
		fields                 []string
	}{
		{"wildcard", "SELECT r.* FROM rules r", "rules", "", []string{"id", "groups"}},
		{"closed-widening", "SELECT r.id FROM rules r", "rules", "absent", []string{"id", "groups"}},
		{"closed-missing", "SELECT r.id,r.groups FROM rules r", "rules", "incomplete", []string{"id"}},
		{"duplicate", "SELECT r.* FROM rules r", "rules", "ambiguous", []string{"id", "id"}},
		{"wrong-table", "SELECT r.* FROM rules r", "other", "does not match", []string{"id"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{SQL: tc.sql}}
			component := &spec.Component{RootView: view}
			fields := []*spec.Column{}
			for _, name := range tc.fields {
				fields = append(fields, &spec.Column{Name: name, Type: spec.TypeRef{Name: "string"}})
			}
			err := RefinePrepared(context.Background(), component, nil, func(_ context.Context, request PreparedColumnRequest) (*PreparedColumnResult, error) {
				request.Source.SQL = "caller mutation"
				return &PreparedColumnResult{Table: tc.table, Columns: fields}, nil
			}, nil)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("got %v, want %s", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if view.Source.SQL != tc.sql {
				t.Fatal("SQL changed")
			}
			view.Columns[0].Name = "changed"
			if fields[0].Name != "id" {
				t.Fatal("caller columns were not detached")
			}
		})
	}
}
