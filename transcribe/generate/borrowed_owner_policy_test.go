package generate

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestBorrowedOwnerPlanNormalizesOnlyDetachedOccurrencePolicies(t *testing.T) {
	for _, policy := range []string{"update", "insert-delete", "assigned-update", "source-row", "source-slice"} {
		t.Run(policy, func(t *testing.T) {
			view := &spec.View{Name: "Rows", TypeName: "Row", Cardinality: spec.CardinalityMany, Source: &spec.ViewSource{Table: "records", SQL: "SELECT id,name,NULL AS note FROM records"}, Columns: []*spec.Column{
				{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}},
				{Name: "name", Source: "name", Type: spec.TypeRef{Name: "string"}},
				{Name: "note", Source: "note", Expression: "NULL", Type: spec.TypeRef{Name: "*string"}, Nullable: true, Tag: `sqlx:"-"`},
			}}
			switch policy {
			case "update":
				view.WriterActionPolicy = policy
			case "insert-delete":
				view.WriterActionPolicy = policy
				view.Columns = append(view.Columns, &spec.Column{Name: "remove", Source: "remove", Type: spec.TypeRef{Name: "bool"}, DeleteMarker: true, Tag: `sqlx:"-"`})
				view.Source.SQL = "SELECT id,name,NULL AS note,0 AS remove FROM records"
			case "assigned-update":
				view.WriterIdentityPolicy = policy
			default:
				view.QueueContract = policy
			}
			component := &spec.Component{Name: "Owner", RootView: view, Routes: []*spec.Route{{Method: "PATCH", Path: "/records"}}, Settings: &spec.Settings{Mutation: "patch", DefaultConnector: "main"}}
			input := Input{Component: component, TargetPackage: "example.com/api"}
			require.NoError(t, input.ValidateLifecycleTarget(true), "actual target must remain independently valid")
			before, err := json.Marshal(component)
			require.NoError(t, err)
			direct, err := BorrowedLeafContractFor(component, view, "example.com/api", "Row", "main", "", "main", "records")
			require.NoError(t, err)
			ownerPlan, err := BorrowedOwnerPlan(input)
			require.NoError(t, err, "synthetic comparison POST must not retain the authored PATCH policy")
			fromPlan, err := BorrowedLeafContractFromPlan(ownerPlan, view, "example.com/api", "Row", "main", "", "main", "records")
			require.NoError(t, err)
			require.NoError(t, CompareBorrowedLeafContracts(direct, fromPlan), "cached/full-owner plan changed canonical row/API")
			require.Equal(t, direct.Fields, fromPlan.Fields)
			require.Equal(t, direct.MarkerFields, fromPlan.MarkerFields)
			require.Equal(t, component, input.Component, "prepared input component was replaced")
			after, err := json.Marshal(component)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after), "comparison changed authored policies, columns or SQL")
			require.NoError(t, input.ValidateLifecycleTarget(true), "comparison changed actual target admission")
		})
	}
}

func TestBorrowedOwnerPlanDoesNotAdmitInvalidActualTarget(t *testing.T) {
	view := &spec.View{Name: "Rows", TypeName: "Row", WriterActionPolicy: "update", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "id", Source: "id", PrimaryKey: true, Type: spec.TypeRef{Name: "int"}}}}
	component := &spec.Component{Name: "Owner", RootView: view, Routes: []*spec.Route{{Method: "POST", Path: "/records"}}, Settings: &spec.Settings{Mutation: "post"}}
	input := Input{Component: component, TargetPackage: "example.com/api"}
	require.Error(t, input.ValidateLifecycleTarget(true), "UPDATE policy is invalid on the actual POST target")
	before, err := json.Marshal(component)
	require.NoError(t, err)
	_, err = BorrowedOwnerPlan(input)
	require.NoError(t, err, "detached row comparison is not execution-target admission")
	require.Error(t, input.ValidateLifecycleTarget(true), "comparison must not make the invalid actual target valid")
	after, err := json.Marshal(component)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after))
}
