// Package txread coordinates native reads sharing a single transaction connection.
package txread

import (
	"context"
	"database/sql"
	"sync"
)

type gate struct {
	slot  chan struct{}
	users int
}

// Entries live only while an operation holds or waits for the connection. Keying
// by the actual transaction also covers caller-owned transactions shared across
// components and contexts, without serializing independent transactions.
var active = struct {
	sync.Mutex
	gates map[*sql.Tx]*gate
}{gates: make(map[*sql.Tx]*gate)}

// Acquire reserves a transaction until its result rows and statement are closed.
// It does not own transaction completion or coordinate caller-issued raw SQL.
func Acquire(ctx context.Context, tx *sql.Tx) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tx == nil {
		return func() {}, nil
	}
	active.Lock()
	g := active.gates[tx]
	if g == nil {
		g = &gate{slot: make(chan struct{}, 1)}
		active.gates[tx] = g
	}
	g.users++
	active.Unlock()
	drop := func() {
		active.Lock()
		g.users--
		if g.users == 0 {
			delete(active.gates, tx)
		}
		active.Unlock()
	}
	select {
	case g.slot <- struct{}{}:
		var once sync.Once
		release := func() { once.Do(func() { <-g.slot; drop() }) }
		if err := ctx.Err(); err != nil {
			release()
			return nil, err
		}
		return release, nil
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	}
}
