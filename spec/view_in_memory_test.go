package spec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemoryRuntimeSourcePreservesDiscovery(t *testing.T) {
	view := &View{InMemory: true, Source: &ViewSource{
		SQL: "SELECT 0 AS id", Table: "placeholder", URI: "discovery.sql",
		Embeds:   []*EmbeddedSQLRef{{Path: "shape.sql"}},
		Bindings: &ViewBindings{Connector: "main"},
	}}
	source := view.RuntimeSource()
	require.Empty(t, source.SQL)
	require.Empty(t, source.Table)
	require.Empty(t, source.URI)
	require.Empty(t, source.Embeds)
	require.Equal(t, "main", source.Bindings.Connector)
	require.Equal(t, "SELECT 0 AS id", view.Source.SQL)
	require.Len(t, view.Source.Embeds, 1)
	view.InMemory = false
	require.Same(t, view.Source, view.RuntimeSource())
}
