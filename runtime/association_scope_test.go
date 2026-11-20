package runtime

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type associationLookupProbe struct{ calls int }

func (s *associationLookupProbe) LookupAssociationDelta(xhandler.ComponentTarget, reflect.Type, string, string) (xhandler.AssociationDelta, bool) {
	s.calls++
	return nil, true
}

func TestAssociationLookupIsBoundToInvocationRoute(t *testing.T) {
	parent := exec.ComponentTarget{Component: spec.Key{Scope: "writer", Name: "Save"}, Route: spec.RouteRef{Method: "PATCH", Path: "/save"}}
	other := parent
	other.Route.Path = "/other"
	lookup := &associationLookupProbe{}
	ctx, err := enterComponent(context.WithValue(t.Context(), xhandler.AssociationDeltaLookupKey, lookup), parent)
	require.NoError(t, err)
	target := xhandler.ComponentTarget{Scope: "writer", Name: "Save", Method: "PATCH", Path: "/save"}
	_, ok := xhandler.LookupAssociationDelta(ctx, target, reflect.TypeFor[int](), "Links", "Rows[0].Links")
	require.True(t, ok)
	alternate := target
	alternate.Path = "/other"
	_, ok = xhandler.LookupAssociationDelta(ctx, alternate, reflect.TypeFor[int](), "Links", "Rows[0].Links")
	require.False(t, ok)
	child, err := enterComponent(ctx, other)
	require.NoError(t, err)
	for _, candidate := range []xhandler.ComponentTarget{target, alternate} {
		_, ok = xhandler.LookupAssociationDelta(child, candidate, reflect.TypeFor[int](), "Links", "Rows[0].Links")
		require.False(t, ok)
	}
	require.Equal(t, 1, lookup.calls)
}
