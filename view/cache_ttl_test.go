package view

import (
	"context"
	"testing"
)

func TestCacheTTLValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider string
		ttl      int
		valid    bool
	}{
		{"afs milliseconds", "", 500, true},
		{"afs zero", "", 0, false},
		{"afs negative", "", -1, false},
		{"aerospike seconds", "aerospike://localhost:3000/test", 1000, true},
		{"aerospike subsecond", "aerospike://localhost:3000/test", 500, false},
		{"aerospike fractional", "aerospike://localhost:3000/test", 1500, false},
		{"aerospike preserve ttl sentinel", "aerospike://localhost:3000/test", 4294967294000, false},
		{"aerospike never expire sentinel", "aerospike://localhost:3000/test", 4294967295000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Cache{Provider: tc.provider, TimeToLiveMs: tc.ttl}
			if err := c.validateTTL(); (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
		})
	}
}

func TestInvalidCacheTTLCannotBeAcceptedOnRetry(t *testing.T) {
	c := &Cache{Provider: "aerospike://localhost:3000/test", Location: "items", TimeToLiveMs: 500}
	for i := 0; i < 2; i++ {
		if err := c.init(context.Background(), nil, nil); err == nil {
			t.Fatal("invalid TTL accepted")
		}
	}
}
