package reader

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"
	xstate "github.com/viant/xdatly/state"
)

// Exercise the ordinary relation pipeline, including its query matcher, with
// child selectors and a conditional SQL source. Builder-only execution does not
// cover that second stage. Fixed tenant/parent collisions qualify native tuple
// membership independently of projection and ordering.
func TestChildSelectorsConditionalSourcePreserveMembership(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := t.Context()
	require.NoError(t, h.ExecStatements(ctx,
		`CREATE TABLE parents(tenant INTEGER,id INTEGER,name TEXT,PRIMARY KEY(tenant,id))`,
		`CREATE TABLE children(id INTEGER,tenant INTEGER,parent_id INTEGER,name TEXT,payload TEXT,PRIMARY KEY(tenant,id),FOREIGN KEY(tenant,parent_id) REFERENCES parents(tenant,id))`,
		`INSERT INTO parents VALUES(3,7,'parent'),(3,8,'other'),(4,7,'foreign')`,
		`INSERT INTO children VALUES(1,3,7,'zeta','private-z'),(2,3,7,'alpha','private-a'),(3,4,7,'foreign','must not return'),(4,3,8,'other-parent','must not return')`,
	))
	type input struct{ ID int }
	type child struct {
		ID            int `sqlx:"id,primaryKey=true"`
		Tenant        int `sqlx:"tenant,primaryKey=true,refTable=parents,refColumn=tenant"`
		ParentID      int `sqlx:"parent_id,refTable=parents,refColumn=id"`
		Name, Payload string
	}
	type parent struct {
		Tenant   int `sqlx:"tenant,primaryKey=true"`
		ID       int `sqlx:"id,primaryKey=true"`
		Name     string
		Children []*child `view:"children,selectorProjection=true,selectorOrderBy=true,match=read_matched" on:"Tenant:tenant=Tenant:tenant,ID:id=ParentID:parent_id" sql:"SELECT c.* FROM (#if($ID == 7) SELECT id,tenant,parent_id,name,payload FROM children #else SELECT id,tenant,parent_id,name,payload FROM children WHERE 1=0 #end) c"`
	}
	type output struct{ Data []*parent }
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/graphs", Name: "Parents"}, Name: "Parents",
		RootView:   &spec.View{Source: &spec.ViewSource{SQL: "SELECT tenant,id,name FROM parents WHERE tenant=3 AND id=:ID"}},
		Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	artifact, err := buildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output](), DirectViewField: "Data"})
	require.NoError(t, err)
	type useCase struct {
		description string
		selector    xstate.Selector
		projected   bool
	}
	for _, tc := range []useCase{
		{description: "child ordering", selector: xstate.Selector{Fields: []string{"ID", "Tenant", "ParentID", "Name", "Payload"}, OrderBy: "name ASC"}},
		{description: "child field selection", selector: xstate.Selector{Fields: []string{"ID", "Tenant", "ParentID", "Name"}}, projected: true},
		{description: "child field selection and ordering", selector: xstate.Selector{Fields: []string{"ID", "Tenant", "ParentID", "Name"}, OrderBy: "name ASC"}, projected: true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			session := &Session{Component: component, OutputType: reflect.TypeFor[output](), Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &rsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithPathParams(map[string]string{"id": "7"}).WithQuery(url.Values{}), Providers: selectorProviders(xstate.Selectors{&xstate.NamedSelector{Name: "children", Selector: tc.selector}})}
			actual, err := NewService().Read(ctx, session)
			require.NoError(t, err)
			result := actual.(*output)
			require.Len(t, result.Data, 1)
			require.Len(t, result.Data[0].Children, 2)
			for _, row := range result.Data[0].Children {
				require.Positive(t, row.ID)
				require.Equal(t, 3, row.Tenant)
				require.Equal(t, 7, row.ParentID)
			}
			names := []string{result.Data[0].Children[0].Name, result.Data[0].Children[1].Name}
			if tc.selector.OrderBy != "" {
				require.Equal(t, []string{"alpha", "zeta"}, names)
			} else {
				require.ElementsMatch(t, []string{"alpha", "zeta"}, names)
			}
			if tc.projected {
				for _, row := range result.Data[0].Children {
					require.Empty(t, row.Payload)
				}
			}
		})
	}
}
