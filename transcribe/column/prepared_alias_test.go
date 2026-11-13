package column

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestPreparedPhysicalAliasParity(t *testing.T) {
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT r.ADVERTISER_ID AS advertiserId FROM records r"}, Columns: []*spec.Column{{Name: "advertiserId", Source: "advertiserId"}}}
	component := &spec.Component{RootView: view}
	resolve := func(context.Context, PreparedColumnRequest) (*PreparedColumnResult, error) {
		return &PreparedColumnResult{Table: "records", Columns: []*spec.Column{{Name: "advertiserId", Source: "ADVERTISER_ID", Type: spec.TypeRef{Name: "int"}, NotNull: true}}}, nil
	}
	for i := 0; i < 2; i++ {
		require.NoError(t, RefinePrepared(t.Context(), component, nil, resolve, nil))
		require.Equal(t, "ADVERTISER_ID", view.Columns[0].Source)
		require.Contains(t, view.Columns[0].Tag, `sqlx:"ADVERTISER_ID|advertiserId`)
	}
}

func TestPreparedPhysicalAliasAuthority(t *testing.T) {
	for _, tc := range []struct{ name, SQL, table, source, tag, want, errorText string }{
		{"nested", "SELECT nested.public_id FROM (SELECT ID AS public_id FROM records) nested", "records", "ID", "", "ID", ""},
		{"authored tag", "SELECT ID AS public_id FROM records", "records", "ID", `sqlx:"owned"`, "ID", ""},
		{"no table authority", "SELECT ID AS public_id FROM records", "", "ID", "", "public_id", ""},
		{"computed", "SELECT ID+1 AS public_id FROM records", "records", "ID", "", "public_id", ""},
		{"joined", "SELECT other.ID AS public_id FROM records r JOIN others other ON other.ID=r.ID", "records", "ID", "", "public_id", ""},
		{"wrong source", "SELECT ID AS public_id FROM records", "records", "WRONG", "", "", "disagrees with SQL lineage"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: tc.SQL}, Columns: []*spec.Column{{Name: "public_id", Source: "public_id", Tag: tc.tag}}}
			err := RefinePrepared(t.Context(), &spec.Component{RootView: view}, nil, func(context.Context, PreparedColumnRequest) (*PreparedColumnResult, error) {
				return &PreparedColumnResult{Table: tc.table, Columns: []*spec.Column{{Name: "public_id", Source: tc.source, Type: spec.TypeRef{Name: "int"}}}}, nil
			}, nil)
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, view.Columns[0].Source)
			if tc.tag != "" {
				require.Equal(t, tc.tag, view.Columns[0].Tag)
			}
			require.Equal(t, tc.SQL, view.Source.SQL)
		})
	}
}

func TestPreparedConflictingPhysicalFacts(t *testing.T) {
	view := &spec.View{Name: "Rows", Source: &spec.ViewSource{SQL: "SELECT ID AS a, ID AS b FROM records"}}
	err := RefinePrepared(t.Context(), &spec.Component{RootView: view}, nil, func(context.Context, PreparedColumnRequest) (*PreparedColumnResult, error) {
		return &PreparedColumnResult{Table: "records", Columns: []*spec.Column{
			{Name: "a", Source: "ID", Type: spec.TypeRef{Name: "int"}, PrimaryKey: true},
			{Name: "b", Source: "ID", Type: spec.TypeRef{Name: "int"}},
		}}, nil
	}, nil)
	require.ErrorContains(t, err, "conflicting prepared physical facts")
}
