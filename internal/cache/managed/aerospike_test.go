package managed

import (
	"context"
	"fmt"
	"sync"
	"testing"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/stretchr/testify/require"
)

type memoryRecords struct {
	sync.Mutex
	bins        as.BinMap
	expirations []uint32
	failure     error
}

func (s *memoryRecords) Get(_ *as.BasePolicy, _ *as.Key, _ ...string) (*as.Record, error) {
	s.Lock()
	defer s.Unlock()
	if s.failure != nil {
		return nil, s.failure
	}
	if s.bins == nil {
		return nil, types.NewAerospikeError(types.KEY_NOT_FOUND_ERROR)
	}
	bins := as.BinMap{}
	for k, v := range s.bins {
		bins[k] = v
	}
	return &as.Record{Bins: bins}, nil
}
func (s *memoryRecords) Put(policy *as.WritePolicy, _ *as.Key, bins as.BinMap) error {
	s.Lock()
	defer s.Unlock()
	if s.failure != nil {
		return s.failure
	}
	if s.bins == nil {
		s.bins = as.BinMap{}
	}
	for k, v := range bins {
		s.bins[k] = v
	}
	s.expirations = append(s.expirations, policy.Expiration)
	return nil
}
func TestAerospikeGenerationScopesPersistWithoutTTL(t *testing.T) {
	records := &memoryRecords{}
	first, second := &AerospikeStore{client: records}, &AerospikeStore{client: records}
	initial, err := first.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, Generation{All: "0", Lazy: "0", Warmup: "0"}, initial)
	var wg sync.WaitGroup
	for _, scope := range []Scope{All, Lazy, Warmup} {
		wg.Add(1)
		go func(scope Scope) {
			defer wg.Done()
			_, err := first.Rotate(context.Background(), scope)
			require.NoError(t, err)
		}(scope)
	}
	wg.Wait()
	current, err := second.Read(context.Background())
	require.NoError(t, err)
	require.NotEqual(t, "0", current.All)
	require.NotEqual(t, "0", current.Lazy)
	require.NotEqual(t, "0", current.Warmup)
	for _, expiration := range records.expirations {
		require.Equal(t, uint32(as.TTLDontExpire), expiration)
	}
	token, err := second.Rotate(context.Background(), Lazy)
	require.NoError(t, err)
	next, err := first.Read(context.Background())
	require.NoError(t, err)
	require.Equal(t, token, next.Lazy)
	require.Equal(t, current.All, next.All)
	require.Equal(t, current.Warmup, next.Warmup)
}
func TestAerospikeGenerationErrorsDoNotResetEpoch(t *testing.T) {
	records := &memoryRecords{failure: fmt.Errorf("offline")}
	store := &AerospikeStore{client: records}
	_, err := store.Read(context.Background())
	require.ErrorContains(t, err, "offline")
	_, err = store.Rotate(context.Background(), All)
	require.ErrorContains(t, err, "offline")
	records.failure = nil
	records.bins = as.BinMap{"all": "invalid"}
	_, err = store.Read(context.Background())
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.Rotate(ctx, All)
	require.ErrorIs(t, err, context.Canceled)
}
