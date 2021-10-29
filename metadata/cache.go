package metadata

import "time"

//Cache represents a cache
type Cache struct {
	Service string //register cache service name
	TTLMs   int
	TTL     time.Duration
}

//Init initialises cache object
func (c *Cache) Init() {
	if c.TTLMs > 0 {
		c.TTL = time.Millisecond * time.Duration(c.TTLMs)
	}
}
