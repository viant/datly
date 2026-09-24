// Package memory implements a concurrent, in-process byte cache.
package memory

import (
	"container/list"
	"context"
	"fmt"
	"sync"
	"time"

	xcache "github.com/viant/xdatly/cache"
)

// Cache evicts the oldest inserted entry when capacity is reached. Updating
// an entry keeps its insertion position. Expired entries are removed on Get.
type Cache struct {
	mu       sync.Mutex
	capacity int
	entries  map[string]*entry
	order    *list.List
}

type entry struct {
	value   []byte
	expires time.Time
	order   *list.Element
}

var _ xcache.Cache = (*Cache)(nil)

// New creates a cache with at most capacity entries. Zero means unlimited.
func New(capacity int) (*Cache, error) {
	if capacity < 0 {
		return nil, fmt.Errorf("memory cache capacity must be nonnegative: %d", capacity)
	}
	return &Cache{capacity: capacity, entries: make(map[string]*entry), order: list.New()}, nil
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	item, ok := c.entries[key]
	if !ok {
		return nil, false, nil
	}
	if !item.expires.IsZero() && !time.Now().Before(item.expires) {
		c.remove(key, item)
		return nil, false, nil
	}
	return append([]byte(nil), item.value...), true, nil
}

func (c *Cache) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl < 0 {
		return fmt.Errorf("memory cache TTL must be nonnegative: %s", ttl)
	}
	var expires time.Time
	if ttl > 0 {
		expires = time.Now().Add(ttl)
	}
	copyValue := append([]byte(nil), value...)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if item, ok := c.entries[key]; ok {
		item.value, item.expires = copyValue, expires
		return nil
	}
	if c.capacity > 0 && len(c.entries) == c.capacity {
		// Reclaim expired entries before evicting a live FIFO entry.
		for element := c.order.Front(); element != nil; {
			next := element.Next()
			oldKey := element.Value.(string)
			old := c.entries[oldKey]
			if !old.expires.IsZero() && !time.Now().Before(old.expires) {
				c.remove(oldKey, old)
			}
			element = next
		}
		if len(c.entries) == c.capacity {
			oldest := c.order.Front()
			oldKey := oldest.Value.(string)
			c.remove(oldKey, c.entries[oldKey])
		}
	}
	c.entries[key] = &entry{value: copyValue, expires: expires, order: c.order.PushBack(key)}
	return nil
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if item, ok := c.entries[key]; ok {
		c.remove(key, item)
	}
	return nil
}

func (c *Cache) remove(key string, item *entry) {
	delete(c.entries, key)
	c.order.Remove(item.order)
}
