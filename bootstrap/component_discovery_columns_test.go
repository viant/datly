package bootstrap

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

type hiddenDiscoveryRow struct {
	Binary         []byte             `json:"-" sqlx:"binary_body"`
	NullableBinary *[]byte            `json:"-" sqlx:"nullable_body"`
	Strings        []string           `json:"-" sqlx:"strings"`
	Nested         [][]byte           `json:"-" sqlx:"nested"`
	Opaque         any                `internal:"true" sqlx:"opaque"`
	Values         map[string]string  `json:"-" sqlx:"values"`
	ID             int                `sqlx:"id" json:"id"`
	Lease          *string            `internal:"true" json:"-" sqlx:"lease_until_raw"`
	Cleanup        string             `internal:"true" json:"-" sqlx:"cleanup_status"`
	JSONHidden     *string            `json:"-" sqlx:"hidden_text"`
	InternalOnly   string             `internal:"true" sqlx:"internal_text"`
	Unmapped       string             `json:"-"`
	Transient      bool               `internal:"true" json:"-" sqlx:"-"`
	Presence       *struct{ ID bool } `setMarker:"true" json:"-"`
}
type hiddenDiscoveryOutput struct {
	Data []*hiddenDiscoveryRow `parameter:"Data,kind=output,in=view" view:"Rows" sql:"SELECT r.* FROM (SELECT id, CAST(value AS CHAR) AS lease_until_raw, LOWER(value) AS cleanup_status, value AS hidden_text, value AS internal_text FROM records) r"`
}

func TestLinkedHiddenDiscoveryColumnsDoNotChangeProjection(t *testing.T) {
	typ := reflect.TypeFor[hiddenDiscoveryOutput]()
	component, err := (ContractResolver{OutputType: linkedContractType(typ)}).Resolve()
	require.NoError(t, err)
	require.NoError(t, CompileOutputColumns(component, typ))
	require.Len(t, component.RootView.Columns, 1)
	before := component.RootView.Clone()
	hidden, err := CompileOutputDiscoveryColumns(component, typ)
	require.NoError(t, err)
	require.Len(t, hidden, 6)
	require.Equal(t, "binary_body", hidden[0].Source)
	require.Equal(t, "[]uint8", hidden[0].Type.Name)
	require.Equal(t, "nullable_body", hidden[1].Source)
	require.True(t, hidden[1].Nullable)
	hidden = hidden[2:]
	require.Equal(t, "lease_until_raw", hidden[0].Source)
	require.Equal(t, "string", hidden[0].Type.Name)
	require.True(t, hidden[0].Type.Pointer)
	require.False(t, hidden[1].Type.Pointer)
	for _, c := range hidden {
		require.True(t, c.NameInferred)
		require.False(t, c.ExplicitType)
	}
	require.Equal(t, before, component.RootView)
	projection, err := (ViewProjection{View: component.RootView}).Columns()
	require.NoError(t, err)
	require.Len(t, projection, 1)
	require.Equal(t, "id", projection[0].Name)
}
