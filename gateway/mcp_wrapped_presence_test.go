package gateway

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPBodyPreservesWrappedPatchClears(t *testing.T) {
	type presence struct {
		ID, Tags, Nullable, Omitted bool
	}
	type patch struct {
		ID       int       `json:"id"`
		Tags     []string  `json:"tags,omitempty"`
		Nullable *string   `json:"nullable,omitempty"`
		Omitted  string    `json:"omitted,omitempty"`
		Has      *presence `json:"-"`
	}
	type wrapper struct {
		Kind  string `json:"kind"`
		Patch *patch `json:"patch,omitempty"`
	}
	input := []wrapper{{Kind: "native", Patch: &patch{
		ID: 7, Tags: []string{}, Has: &presence{ID: true, Tags: true, Nullable: true},
	}}}
	encoded, err := json.Marshal(preserveMCPBodyPresence(nil, input))
	require.NoError(t, err)
	require.JSONEq(t, `[{"kind":"native","patch":{"id":7,"tags":[],"nullable":null}}]`, string(encoded))
	// Unmarked models retain their existing omitempty behavior.
	input[0].Patch.Has = nil
	encoded, err = json.Marshal(preserveMCPBodyPresence(nil, input))
	require.NoError(t, err)
	require.JSONEq(t, `[{"kind":"native","patch":{"id":7}}]`, string(encoded))
}
