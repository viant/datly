package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"github.com/viant/tagly/tags"
)

func TestGeneratedColumnPreservesRepeatedEnumAliasesAndChoiceOrder(t *testing.T) {
	const raw = `choice:"1:RUNNING" choice:"0:IDLE" enumAccept:"scalar" enumStyle:"ordinal" enumAlias:"0:0" enumAlias:"1:1" enumAlias:"0:old-idle" enumAlias:"1:old-running" enumCase:"fold"`
	column := &spec.Column{Name: "State", Source: "state", Tag: raw}
	generated := generatedStructTag(scalarColumnFieldTag(column, "state", false))
	parsed, err := tags.Parse(generated)
	require.NoError(t, err)
	var aliases []string
	for _, tag := range parsed {
		if tag.Name == "enumAlias" {
			aliases = append(aliases, string(tag.Values))
		}
	}
	require.Equal(t, []string{"0:0", "1:1", "0:old-idle", "1:old-running"}, aliases)
	choices, err := choice.Tags(reflect.StructTag(generated))
	require.NoError(t, err)
	require.Equal(t, []string{"1:RUNNING", "0:IDLE"}, choices)
	enum, err := choice.ParseEnum(reflect.StructTag(generated), reflect.TypeFor[int]())
	require.NoError(t, err)
	value, err := enum.DecodeJSON([]byte(`"old-running"`))
	require.NoError(t, err)
	require.Equal(t, "1", string(value))
	require.Equal(t, raw, column.Tag, "generation must not mutate source metadata")
}

func TestGeneratedColumnPreservesRepeatedOutputAliasesOnReload(t *testing.T) {
	const raw = `choice:"1:Second" choice:"0:First" enumAccept:"literal" enumStyle:"literal" enumOutputAlias:"0:stored-first" enumOutputAlias:"1:stored-second"`
	column := &spec.Column{Name: "State", Source: "state", Tag: raw}
	generated := generatedStructTag(scalarColumnFieldTag(column, "state", false))
	parsed, err := tags.Parse(generated)
	require.NoError(t, err)
	var aliases []string
	for _, tag := range parsed {
		if tag.Name == "enumOutputAlias" {
			aliases = append(aliases, string(tag.Values))
		}
	}
	require.Equal(t, []string{"0:stored-first", "1:stored-second"}, aliases)
	choices, err := choice.Tags(reflect.StructTag(generated))
	require.NoError(t, err)
	require.Equal(t, []string{"1:Second", "0:First"}, choices)
	enum, err := choice.ParseEnum(reflect.StructTag(generated), reflect.TypeFor[string]())
	require.NoError(t, err)
	value, err := enum.Encode("stored-second")
	require.NoError(t, err)
	require.Equal(t, "Second", value)
	require.Nil(t, enum.WireValues(false))
	require.Equal(t, raw, column.Tag)
}
