package column

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestPreparedSummaryUsesParentSQLWithoutChangingAuthoredSource(t *testing.T) {
	rootSQL := "SELECT r.id FROM records r"
	summarySQL := "SELECT CASE WHEN $View.Limit > 0 THEN COUNT(*) ELSE 0 END AS CNT FROM ($View.Users.NonWindowSQL) parent"
	for _, alias := range []string{"Users", "", "Wrong"} {
		t.Run("alias="+alias, func(t *testing.T) {
			SQL := strings.ReplaceAll(summarySQL, "$View.Users.NonWindowSQL", "$View.NonWindowSQL")
			if alias != "" {
				SQL = strings.ReplaceAll(SQL, "$View.NonWindowSQL", "$View."+alias+".NonWindowSQL")
			}
			summary := &spec.View{Name: "Totals", Namespace: "t", Source: &spec.ViewSource{SQL: SQL}}
			root := &spec.View{Name: "Users", Namespace: "r", Source: &spec.ViewSource{SQL: rootSQL}, Relations: []*spec.Relation{{Kind: spec.RelationKindDerived, Holder: "Totals", View: summary}}}
			component := &spec.Component{Name: "Users", RootView: root}
			calls := 0
			err := RefinePrepared(t.Context(), component, nil, func(_ context.Context, request PreparedColumnRequest) (*PreparedColumnResult, error) {
				calls++
				name := "id"
				if request.Name == "Totals" {
					name = "CNT"
					require.Equal(t, SQL, request.Source.SQL, "resolver sees detached authored SQL")
				}
				return &PreparedColumnResult{Columns: []*spec.Column{{Name: name, Type: spec.TypeRef{Name: "int"}}}}, nil
			}, nil)
			if alias == "Wrong" {
				require.ErrorContains(t, err, "unknown parent view alias")
				require.Equal(t, 1, calls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, calls, "one prepared metadata lookup per view; no database resolver")
			require.Len(t, summary.Columns, 1)
			require.Equal(t, SQL, summary.Source.SQL)
			require.Equal(t, rootSQL, root.Source.SQL)
		})
	}
}

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

func TestPreparedCompositePredicatePreservesSource(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		valid     bool
	}{
		{"generated current", `SELECT r.id FROM records r WHERE $criteria.CompositeIn("r", $Unsafe.ProjectKeys($Body, $Current))`, true},
		{"projection rejected", `SELECT $criteria.CompositeIn("r", $Keys) AS id FROM records r`, false},
		{"invalid arity", `SELECT r.id FROM records r WHERE $criteria.CompositeIn("r")`, false},
		{"chained call", `SELECT r.id FROM records r WHERE $criteria.CompositeIn("r", $Keys).Other()`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Current", Source: &spec.ViewSource{SQL: tc.sql}}
			calls := 0
			err := RefinePrepared(t.Context(), &spec.Component{RootView: view}, nil, func(_ context.Context, request PreparedColumnRequest) (*PreparedColumnResult, error) {
				calls++
				require.Equal(t, tc.sql, request.Source.SQL)
				return &PreparedColumnResult{Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}, nil
			}, nil)
			require.Equal(t, tc.sql, view.Source.SQL)
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.Len(t, view.Columns, 1)
			} else {
				require.Error(t, err)
				require.Zero(t, calls)
			}
		})
	}
}
