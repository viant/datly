package compile

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
)

// The runtime already owns repeated aliases. DQL must preserve their complete
// ordered group exactly as it preserves repeated choice declarations.
func TestReaderRepeatedEnumAliasNativeDQL(t *testing.T) {
	const metadata = `choice:"0:IDLE" choice:"1:RUNNING" enumAccept:"scalar" enumStyle:"ordinal" enumAlias:"0:0" enumAlias:"1:1" enumAlias:"0:old-idle" enumAlias:"1:old-running" enumCase:"fold"`
	const SQL = `SELECT r.*, CAST(r.state AS 'int'), tag(r.state,'` + metadata + `') FROM (SELECT state FROM records) r`
	input := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}}
	before := input.Clone()
	compiled, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
	require.NoError(t, err)
	require.Equal(t, before, input)
	require.Len(t, compiled.Columns, 1)
	enum, err := choice.ParseEnum(reflect.StructTag(compiled.Columns[0].Tag), reflect.TypeFor[int]())
	require.NoError(t, err)
	for raw, want := range map[string]string{`0`: `0`, `1`: `1`, `"0"`: `0`, `"1"`: `1`, `"OLD-IDLE"`: `0`, `"old-running"`: `1`} {
		actual, err := enum.DecodeJSON([]byte(raw))
		require.NoError(t, err)
		require.Equal(t, want, string(actual))
	}
	merged, err := mergeDeclarationTags(compiled.Columns[0].Tag, metadata, "r.state")
	require.NoError(t, err)
	require.Equal(t, compiled.Columns[0].Tag, merged, "equivalent declarations must not duplicate aliases")
}

func TestReaderEnumAliasInvalidDuplicatesRetainNativeOwner(t *testing.T) {
	const prefix = `choice:"0:IDLE" choice:"1:RUNNING" enumAccept:"scalar" enumCase:"fold" `
	for _, raw := range []string{
		prefix + `enumAlias:"0:shared" enumAlias:"1:SHARED"`,
		prefix + `enumAlias:"2:missing"`,
	} {
		_, err := choice.ParseEnum(reflect.StructTag(raw), reflect.TypeFor[int]())
		require.Error(t, err, "DQL tag preservation must not weaken native alias validation")
	}
	_, err := mergeDeclarationTags(`enumAlias:"0:old"`, `enumAlias:"0:new"`, "r.state")
	require.ErrorContains(t, err, "conflicting")
}

func TestReaderRepeatedOutputEnumAliasNativeDQL(t *testing.T) {
	const metadata = `choice:"0:First" choice:"1:Second" enumAccept:"literal" enumStyle:"literal" enumOutputAlias:"0:stored-first" enumOutputAlias:"1:stored-second"`
	const SQL = `SELECT r.*, CAST(r.state AS 'string'), tag(r.state,'` + metadata + `') FROM (SELECT state FROM records) r`
	input := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}}
	before := input.Clone()
	compiled, err := NewReader().Compile(ReadInput{View: input, SQL: SQL})
	require.NoError(t, err)
	require.Equal(t, before, input)
	require.Len(t, compiled.Columns, 1)
	enum, err := choice.ParseEnum(reflect.StructTag(compiled.Columns[0].Tag), reflect.TypeFor[string]())
	require.NoError(t, err)
	for stored, want := range map[string]string{"stored-first": "First", "stored-second": "Second", "future": "future"} {
		actual, err := enum.Encode(stored)
		require.NoError(t, err)
		require.Equal(t, want, actual)
		decoded, err := enum.DecodeJSON([]byte(`"` + stored + `"`))
		require.NoError(t, err)
		require.Equal(t, `"`+stored+`"`, string(decoded), "output aliases must not normalize input")
	}
	require.Nil(t, enum.WireValues(false), "passthrough output requires an open schema")
	merged, err := mergeDeclarationTags(compiled.Columns[0].Tag, metadata, "r.state")
	require.NoError(t, err)
	require.Equal(t, compiled.Columns[0].Tag, merged)
	_, err = mergeDeclarationTags(compiled.Columns[0].Tag, `enumOutputAlias:"0:changed"`, "r.state")
	require.ErrorContains(t, err, "conflicting ordered enumOutputAlias")
}
