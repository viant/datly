package managed

import (
	"context"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/io/read/cache/aerospike"
)

// Use a dedicated test server: the test creates short-lived cache data and
// removes only the generation-control records it created.
func TestManagedAerospikeIntegration(t *testing.T) {
	endpoint := os.Getenv("DATLY_TEST_AEROSPIKE")
	if endpoint == "" {
		t.Skip("set DATLY_TEST_AEROSPIKE=aerospike://host:port/namespace for a dedicated test server")
	}
	parsed, err := url.Parse(endpoint)
	require.NoError(t, err)
	port, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)
	policy := as.NewClientPolicy()
	policy.Timeout = time.Second
	client, err := as.NewClientWithPolicy(policy, parsed.Hostname(), port)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	namespace := strings.TrimPrefix(parsed.Path, "/")
	for _, grouped := range []bool{false, true} {
		t.Run(map[bool]string{false: "query", true: "indexed"}[grouped], func(t *testing.T) {
			owner := uuid.NewString()
			set := "datly_cache_test"
			store, err := NewAerospikeStore(client, namespace, set, owner)
			require.NoError(t, err)
			t.Cleanup(func() { _, err := client.Delete(nil, store.key); require.NoError(t, err) })
			native, err := aerospike.New(namespace, set, client, 2)
			require.NoError(t, err)
			var lazyCreated, warmupCreated atomic.Int64
			observer := func(kind string, entries int) {
				if kind == string(Warmup) {
					warmupCreated.Add(int64(entries))
				} else {
					lazyCreated.Add(int64(entries))
				}
			}
			first := New(native, store, owner, observer)
			if os.Getenv("DATLY_REQUIRE_NATIVE_CREATION_METRICS") == "1" {
				require.True(t, first.nativeCreation, "native SQLX creation observer must be active")
			}
			secondNative, err := aerospike.New(namespace, set, client, 2)
			require.NoError(t, err)
			secondStore, err := NewAerospikeStore(client, namespace, set, owner)
			require.NoError(t, err)
			second := New(secondNative, secondStore, owner, observer)
			db := database(t)
			column := ""
			if grouped {
				column = "id"
			}
			written, err := first.IndexBy(context.Background(), db, column, "SELECT id,name FROM items", nil)
			require.NoError(t, err)
			expectedWarmup := int64(1)
			if grouped {
				expectedWarmup = 2
			}
			require.Equal(t, int(expectedWarmup), written, "IndexBy must retain native marker counts")
			if first.nativeCreation {
				require.Equal(t, expectedWarmup, warmupCreated.Load())
			}
			require.Zero(t, lazyCreated.Load())
			change(t, db)
			name, stats := query(t, db, second, matcher(grouped), false, nil)
			require.Equal(t, "before", name)
			require.True(t, stats.FoundWarmup)
			name, _ = query(t, db, first, matcher(grouped), true, nil)
			require.Equal(t, "after", name)
			name, stats = query(t, db, second, matcher(grouped), false, nil)
			require.Equal(t, "after", name)
			require.True(t, stats.FoundLazy)
			require.Equal(t, int64(1), lazyCreated.Load(), "hits must not create entries")
			_, err = second.Invalidate(context.Background(), Lazy)
			require.NoError(t, err)
			name, stats = query(t, db, first, nil, false, nil)
			require.Equal(t, "after", name)
			require.False(t, stats.FoundAny())
			_, err = db.Exec("UPDATE items SET name='expired'")
			require.NoError(t, err)
			key, err := as.NewKey(namespace, set, stats.Key)
			require.NoError(t, err)
			stored, err := client.Get(nil, key)
			require.NoError(t, err)
			require.NotNil(t, stored)
			require.Greater(t, stored.Expiration, uint32(0))
			require.LessOrEqual(t, stored.Expiration, uint32(2))
			// Server expiry uses whole seconds; avoid assuming a subsecond
			// alignment between the client clock and the server clock.
			deadline := time.Now().Add(5 * time.Second)
			for {
				name, stats = query(t, db, second, nil, false, nil)
				if name == "expired" || time.Now().After(deadline) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			require.Equal(t, "expired", name)
			require.False(t, stats.FoundAny())
			_, err = second.Invalidate(context.Background(), All)
			require.NoError(t, err)
			_, err = db.Exec("UPDATE items SET name='warm-before'")
			require.NoError(t, err)
			warm(t, db, first, grouped)
			_, err = db.Exec("UPDATE items SET name='warm-after'")
			require.NoError(t, err)
			name, stats = query(t, db, second, matcher(grouped), false, nil)
			require.Equal(t, "warm-before", name)
			require.True(t, stats.FoundWarmup)
			deadline = time.Now().Add(5 * time.Second)
			for {
				name, stats = query(t, db, second, matcher(grouped), false, nil)
				if name == "warm-after" || time.Now().After(deadline) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			require.Equal(t, "warm-after", name)
			require.False(t, stats.FoundAny())
			generation, err := store.Read(context.Background())
			require.NoError(t, err)
			require.NotEqual(t, "0", generation.All)
			require.Equal(t, int64(4), lazyCreated.Load())
			if first.nativeCreation {
				require.Equal(t, 2*expectedWarmup, warmupCreated.Load())
			}
		})
	}
}
