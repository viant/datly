package data

import "time"

//Cache represents a cache
type Cache struct {
	Service string //register cache service name
	TTLMs   int
	TTL     time.Duration
}
