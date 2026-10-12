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

func TestLinkedHiddenMappedColumnsRetainDiscoveryTypes(t *testing.T) {
	typ := reflect.TypeFor[hiddenDiscoveryOutput]()
	component, err := (ContractResolver{OutputType: linkedContractType(typ)}).Resolve()
	require.NoError(t, err)
	require.NoError(t, CompileOutputColumns(component, typ))
	columns := component.RootView.Columns
	found := map[string]bool{}
	for _, column := range columns {
		found[column.Source] = true
	}
	for _, name := range []string{"binary_body", "nullable_body", "lease_until_raw", "cleanup_status", "hidden_text", "internal_text"} {
		require.True(t, found[name], name)
	}
	require.False(t, found["Unmapped"])
	for _, column := range columns {
		if column.Source == "cleanup_status" {
			require.Contains(t, column.Tag, `json:"-"`)
		}
	}
}
