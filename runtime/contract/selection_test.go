package contract

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	xcontract "github.com/viant/xdatly/contract"
)

func TestProjectedArtifactUsesExistingVariantIncludingUnchangedPolicies(t *testing.T) {
	for _, mode := range []string{"named", "unnamed", "nocache"} {
		t.Run(mode, func(t *testing.T) {
			p, err := NewInput(reflect.TypeFor[struct{ Value string }](), 4, nil)
			require.NoError(t, err)
			require.NoError(t, p.PrepareProjection())
			var calls atomic.Int64
			for i := 0; i < 3; i++ {
				name := "variant"
				if mode == "unnamed" {
					name = ""
				}
				policy := &xcontract.Policy{Name: name, NoCache: mode == "nocache", Field: func(string, reflect.Type, *xcontract.Field) (*xcontract.Field, error) { return nil, nil }}
				representation, _, identity, err := p.ResolvePreparedInputSelection(t.Context(), nil, policy)
				require.NoError(t, err)
				require.Nil(t, representation)
				_, err = identity.PrepareProjection(t.Context(), func() (any, error) { calls.Add(1); return "immutable", nil })
				require.NoError(t, err)
			}
			expected := int64(3)
			if mode == "named" {
				expected = 1
			}
			require.Equal(t, expected, calls.Load())
			if mode != "named" {
				require.Empty(t, p.variants)
			}
		})
	}
}
func TestProjectedArtifactConcurrentReuseAndCanceledPreparation(t *testing.T) {
	p, err := NewInput(reflect.TypeFor[struct{ Value string }](), 2, nil)
	require.NoError(t, err)
	require.NoError(t, p.PrepareProjection())
	policy := &xcontract.Policy{Name: "same", Field: func(string, reflect.Type, *xcontract.Field) (*xcontract.Field, error) { return nil, nil }}
	_, _, identity, err := p.ResolvePreparedInputSelection(t.Context(), nil, policy)
	require.NoError(t, err)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = identity.PrepareProjection(canceled, func() (any, error) { t.Fatal("canceled operation prepared metadata"); return nil, nil })
	require.ErrorIs(t, err, context.Canceled)
	var calls atomic.Int64
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := identity.PrepareProjection(t.Context(), func() (any, error) { calls.Add(1); return "immutable", nil })
			if err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	require.EqualValues(t, 1, calls.Load())
}
