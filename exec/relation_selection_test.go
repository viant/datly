package exec

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestRelationSelectionExactInvocationOwnership(t *testing.T) {
	target := ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Records"}, Route: spec.RouteRef{Method: "GET", Path: "/records"}}
	names := []string{"Label"}
	selection := NewRelationSelection(target, "records", names...)
	names[0] = "Other"
	ctx, err := EnterRelationSelection(context.Background(), target, selection)
	require.NoError(t, err)
	view, actual, ok := SelectedRelations(ctx, target.Component)
	require.True(t, ok)
	require.Equal(t, "records", view)
	require.Equal(t, []string{"Label"}, actual)
	actual[0] = "Changed"
	_, actual, _ = SelectedRelations(ctx, target.Component)
	require.Equal(t, []string{"Label"}, actual)
	child, err := EnterRelationSelection(ctx, target, nil)
	require.NoError(t, err)
	_, _, ok = SelectedRelations(child, target.Component)
	require.False(t, ok, "same-component nested call cannot inherit")
	foreign := target
	foreign.Route.Path = "/other"
	_, err = EnterRelationSelection(ctx, foreign, selection)
	require.Error(t, err)
	empty, err := EnterRelationSelection(ctx, target, NewRelationSelection(target, "records"))
	require.NoError(t, err)
	_, actual, ok = SelectedRelations(empty, target.Component)
	require.True(t, ok)
	require.Empty(t, actual)
}

func TestRelationSelectionIsNotReaderOptionsTransportInput(t *testing.T) {
	target := ComponentTarget{Component: spec.Key{Name: "Reader"}, Route: spec.RouteRef{Method: "GET", Path: "/reader"}}
	raw, err := json.Marshal(ReaderOptions{Relations: NewRelationSelection(target, "root", "Label")})
	require.NoError(t, err)
	require.NotContains(t, string(raw), "Relations")
	var options ReaderOptions
	require.NoError(t, json.Unmarshal([]byte(`{"Relations":{"view":"root","holders":["Label"]}}`), &options))
	require.Nil(t, options.Relations)
}
