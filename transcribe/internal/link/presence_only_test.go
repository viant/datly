package link

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestPresenceOnlyOverlayPreservesExplicitFalse(t *testing.T) {
	yes, no := true, false
	inherited := &spec.Parameter{Name: "Supplied", PresenceOnly: &yes}
	got := overlayParam(inherited, &spec.Parameter{Name: "Supplied", PresenceOnly: &no})
	require.NotNil(t, got.PresenceOnly)
	require.False(t, *got.PresenceOnly)
	require.True(t, *inherited.PresenceOnly)
	got = overlayParam(inherited, &spec.Parameter{Name: "Supplied"})
	require.True(t, *got.PresenceOnly)
}
