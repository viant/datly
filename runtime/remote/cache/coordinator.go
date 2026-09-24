// Package cache coordinates callers of a borrowed xdatly byte cache. It owns
// no backend or cached values; its metadata is local to one owner.
package cache

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	xcache "github.com/viant/xdatly/cache"
)

type Outcome struct {
	Value   []byte
	Expires time.Time
}

type flight struct {
	done  chan struct{}
	value []byte
	err   error
}

type expiryItem struct {
	partition string
	key       string
	expires   time.Time
	index     int
}

type expiryHeap []*expiryItem

func (h expiryHeap) Len() int           { return len(h) }
func (h expiryHeap) Less(i, j int) bool { return h[i].expires.Before(h[j].expires) }
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *expiryHeap) Push(value any) {
	item := value.(*expiryItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *expiryHeap) Pop() any {
	last := len(*h) - 1
	item := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	item.index = -1
	return item
}

// Coordinator shares in-flight fetches and fences writes across local
// invalidations. Backend operations and value validation never hold mu.
type Coordinator struct {
	mu         sync.Mutex
	clock      func() time.Time
	allEpoch   uint64
	partEpoch  map[string]uint64
	inflight   map[string]*flight
	partitions map[string]map[string]*expiryItem
	expires    expiryHeap
}

func New(clock func() time.Time) *Coordinator {
	if clock == nil {
		clock = time.Now
	}
	return &Coordinator{clock: clock, partEpoch: map[string]uint64{}, inflight: map[string]*flight{}, partitions: map[string]map[string]*expiryItem{}}
}

func (c *Coordinator) physical(key, partition string) string {
	return fmt.Sprintf("%d:%d:%s", c.allEpoch, c.partEpoch[partition], key)
}

func (c *Coordinator) track(partition, physical string, expires time.Time) {
	keys := c.partitions[partition]
	if keys == nil {
		keys = map[string]*expiryItem{}
		c.partitions[partition] = keys
	}
	if existing := keys[physical]; existing != nil {
		if !existing.expires.Equal(expires) {
			existing.expires = expires
			heap.Fix(&c.expires, existing.index)
		}
		return
	}
	item := &expiryItem{partition: partition, key: physical, expires: expires}
	keys[physical] = item
	heap.Push(&c.expires, item)
}

func (c *Coordinator) untrack(partition, physical string) {
	if item := c.partitions[partition][physical]; item != nil && item.index >= 0 {
		heap.Remove(&c.expires, item.index)
	}
	delete(c.partitions[partition], physical)
	if len(c.partitions[partition]) == 0 {
		delete(c.partitions, partition)
	}
}

// pruneLocked reaps at most limit expired keys. Normal calls use a fixed batch
// so their mutex work is independent of the number of live principals.
func (c *Coordinator) pruneLocked(limit int) {
	now := c.clock()
	for removed := 0; len(c.expires) > 0 && (limit < 0 || removed < limit); removed++ {
		item := c.expires[0]
		if item.expires.After(now) {
			break
		}
		heap.Pop(&c.expires)
		delete(c.partitions[item.partition], item.key)
		if len(c.partitions[item.partition]) == 0 {
			delete(c.partitions, item.partition)
		}
	}
}

func await(ctx context.Context, running *flight) ([]byte, bool, error) {
	select {
	case <-running.done:
		if running.err != nil {
			return nil, false, running.err
		}
		return append([]byte(nil), running.value...), true, nil
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// Resolve returns a live backend hit or shares one fetch for this generation.
// valid checks the caller's absolute-expiry envelope outside the mutex.
func (c *Coordinator) Resolve(ctx context.Context, backend xcache.Cache, key, partition string, valid func([]byte) (time.Time, bool), fetch func(context.Context) (Outcome, error)) ([]byte, bool, error) {
	if c == nil || backend == nil || valid == nil || fetch == nil {
		return nil, false, fmt.Errorf("cache coordinator, backend, validator and fetch are required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		c.mu.Lock()
		c.pruneLocked(64)
		physical := c.physical(key, partition)
		if running := c.inflight[physical]; running != nil {
			c.mu.Unlock()
			return await(ctx, running)
		}
		c.mu.Unlock()

		value, found, err := backend.Get(ctx, physical)
		if err != nil {
			return nil, false, err
		}
		var expires time.Time
		validEntry := false
		if found {
			expires, validEntry = valid(value)
		}
		c.mu.Lock()
		if physical != c.physical(key, partition) {
			c.mu.Unlock()
			continue
		}
		if validEntry {
			c.track(partition, physical, expires)
			c.mu.Unlock()
			return value, true, nil
		}
		if running := c.inflight[physical]; running != nil {
			c.mu.Unlock()
			return await(ctx, running)
		}
		running := &flight{done: make(chan struct{})}
		c.inflight[physical] = running
		c.mu.Unlock()

		outcome, fetchErr := fetch(ctx)
		if fetchErr == nil && outcome.Expires.After(c.clock()) {
			c.mu.Lock()
			write := physical == c.physical(key, partition)
			if write {
				// Track before Put: invalidation can snapshot this pending key.
				c.track(partition, physical, outcome.Expires)
			}
			c.mu.Unlock()
			if write {
				ttl := outcome.Expires.Sub(c.clock())
				if ttl > 0 {
					fetchErr = backend.Put(ctx, physical, outcome.Value, ttl)
				}
				c.mu.Lock()
				stale := physical != c.physical(key, partition)
				if stale || ttl <= 0 || fetchErr != nil {
					c.untrack(partition, physical)
				}
				c.mu.Unlock()
				if stale && fetchErr == nil && ttl > 0 {
					// Invalidation may have deleted before a slow Put completed.
					if err := backend.Delete(context.WithoutCancel(ctx), physical); err != nil {
						fetchErr = err
					}
				}
			}
		}
		c.mu.Lock()
		delete(c.inflight, physical)
		if fetchErr == nil {
			running.value = append([]byte(nil), outcome.Value...)
		}
		running.err = fetchErr
		c.mu.Unlock()
		close(running.done)
		if fetchErr != nil {
			return nil, false, fetchErr
		}
		return append([]byte(nil), outcome.Value...), false, nil
	}
}

// Invalidate changes the key generation before deleting old physical keys.
// Slow backend deletes cannot block unrelated requests or resurrect old data.
func (c *Coordinator) Invalidate(ctx context.Context, backend xcache.Cache, partition string, all bool) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if c == nil || backend == nil {
		return 0, fmt.Errorf("cache coordinator and backend are required")
	}
	c.mu.Lock()
	c.pruneLocked(-1)
	if all {
		c.allEpoch++
	} else {
		c.partEpoch[partition]++
	}
	var keys []string
	for scope, tracked := range c.partitions {
		if !all && scope != partition {
			continue
		}
		for key := range tracked {
			keys = append(keys, key)
			c.untrack(scope, key)
		}
	}
	c.mu.Unlock()

	removed := 0
	for _, key := range keys {
		_, found, err := backend.Get(ctx, key)
		if err != nil {
			return removed, err
		}
		if err := backend.Delete(ctx, key); err != nil {
			return removed, err
		}
		if found {
			removed++
		}
	}
	return removed, nil
}

// Count reconciles locally tracked entries against backend eviction.
func (c *Coordinator) Count(ctx context.Context, backend xcache.Cache) (int, error) {
	if c == nil || backend == nil {
		return 0, fmt.Errorf("cache coordinator and backend are required")
	}
	c.mu.Lock()
	c.pruneLocked(-1)
	keys := make(map[string]time.Time)
	for _, partition := range c.partitions {
		for key, item := range partition {
			keys[key] = item.expires
		}
	}
	c.mu.Unlock()
	count := 0
	for key, expires := range keys {
		_, found, err := backend.Get(ctx, key)
		if err != nil {
			return 0, err
		}
		if found {
			count++
			continue
		}
		c.mu.Lock()
		for partition, tracked := range c.partitions {
			if item := tracked[key]; item != nil && item.expires.Equal(expires) {
				c.untrack(partition, key)
			}
		}
		c.mu.Unlock()
	}
	return count, nil
}
