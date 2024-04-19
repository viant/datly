package jwt

import (
	"github.com/viant/scy/auth/jwt"
	"sync"
	"time"
)

type cache struct {
	entries map[string]*jwt.Claims
	mux     sync.RWMutex
}

func (c *cache) get(key string) *jwt.Claims {
	c.mux.RLock()
	result, ok := c.entries[key]
	c.mux.RUnlock()
	if !ok {
		return nil
	}
	if result.VerifyExpiresAt(time.Now(), true) {
		return result
	}
	c.mux.Lock()
	delete(c.entries, key)
	c.mux.Unlock()
	return nil
}

func (c *cache) set(key string, value *jwt.Claims) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.entries[key] = value
	if len(c.entries) > 100 {
		c.entries = make(map[string]*jwt.Claims)
	}
}

func newCache() *cache {
	return &cache{
		entries: make(map[string]*jwt.Claims),
	}
}
