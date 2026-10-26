package handlergen

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	hp "github.com/viant/datly/transcribe/handler/ast"
)

func TestPreparedSecondRoleRetainsGeneratedPresence(t *testing.T) {
	public := &spec.View{Name: "requested", Auxiliary: true}
	physical := &spec.View{Name: "prepared", WriterActionPolicy: "insert-delete"}
	root := &spec.View{Name: "owners", Auxiliary: true, Relations: []*spec.Relation{{View: public}, {View: physical}}}
	rootID, _ := root.Identity()
	publicID, _ := public.Identity()
	physicalID, _ := physical.Identity()
	component := &spec.Component{Name: "Owners", RootView: root}
	semantic := &hp.Plan{Root: &hp.RecordPlan{Identity: rootID, Auxiliary: true, Relations: []*hp.RelationPlan{
		{Child: &hp.RecordPlan{Identity: publicID, Auxiliary: true}},
		{Child: &hp.RecordPlan{Identity: physicalID}},
	}}}
	generated := &gen.Plan{Views: []gen.ViewPlan{{Identity: publicID, RoleIdentities: []string{physicalID}, Name: "Member", Ownership: gen.ViewGenerated,
		Fields: []gen.Field{{Name: "Id", Type: "int"}, {Name: "Remove", Type: "bool"}}, SetMarkerFields: []string{"Id", "Remove"}}}}
	g := New(&Context{Component: component}, &gen.Input{Component: component}, Options{Handler: HandlerOptions{Target: HandlerGo, Go: GoHandlerOptions{Execution: GoExecutionMutation}}})
	refined, err := g.withGeneratedPresenceForGraph(semantic, generated, true)
	require.NoError(t, err)
	for _, relation := range refined.Root.Relations {
		require.Equal(t, []string{"Id", "Remove"}, relation.Child.PresenceFields)
		require.NotNil(t, relation.Child.Entity)
		require.Equal(t, "MemberHas", relation.Child.Entity.MarkerType.Name)
	}
}
