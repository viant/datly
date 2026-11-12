// Package predicatereplay retains native predicate evaluations only for one
// root read and its diagnostic probe. It is never shared between invocations.
package predicatereplay

import (
	"context"
	"fmt"
	"github.com/viant/datly/spec"
)

type key struct{}
type entry struct {
	SQL  string
	Args []any
}
type State struct {
	owner     any
	entries   map[any][]entry
	positions map[any]int
	replay    bool
	active    map[*spec.Predicate]bool
	auth      bool
}

func Capture(ctx context.Context) (context.Context, *State) {
	s := &State{entries: map[any][]entry{}, active: map[*spec.Predicate]bool{}}
	return context.WithValue(ctx, key{}, s), s
}
func (s *State) Probe(ctx context.Context) context.Context {
	return context.WithValue(ctx, key{}, &State{owner: s.owner, entries: s.entries, positions: map[any]int{}, replay: true})
}
func Lookup(ctx context.Context, occurrence any, auth bool) (string, []any, bool, error) {
	s, _ := ctx.Value(key{}).(*State)
	if s == nil || !s.replay {
		return "", nil, false, nil
	}
	if auth {
		return "", nil, true, nil
	}
	index := s.positions[occurrence]
	entries := s.entries[occurrence]
	if index >= len(entries) {
		return "", nil, true, fmt.Errorf("predicate probe differs from the authorized evaluation")
	}
	s.positions[occurrence] = index + 1
	return entries[index].SQL, entries[index].Args, true, nil
}
func Record(ctx context.Context, occurrence any, definition *spec.Predicate, sql string, args []any) {
	s, _ := ctx.Value(key{}).(*State)
	if s != nil && !s.replay {
		if definition != nil {
			s.auth = s.auth || definition.Auth
		}
		if sql != "" && definition != nil {
			s.active[definition] = true
		}
		s.entries[occurrence] = append(s.entries[occurrence], entry{SQL: sql, Args: append([]any(nil), args...)})
	}
}

func (s *State) HasAuthorization() bool              { return s != nil && s.auth }
func (s *State) HasPredicate(p *spec.Predicate) bool { return s != nil && s.active[p] }

// Without prevents a nested native reader from inheriting its caller's root
// capture. It allocates no context when no capture is installed.
func Without(ctx context.Context) context.Context {
	if state, _ := ctx.Value(key{}).(*State); state == nil {
		return ctx
	}
	return context.WithValue(ctx, key{}, (*State)(nil))
}

// ForProgram establishes ownership before any custom handler can bind nested
// dependencies. Another program cannot contribute authorization or identities
// to this root's classification, or consume its replay entries.
func ForProgram(ctx context.Context, program any) context.Context {
	state, _ := ctx.Value(key{}).(*State)
	if state == nil {
		return ctx
	}
	if state.owner == nil {
		state.owner = program
	}
	if state.owner != program {
		return Without(ctx)
	}
	return ctx
}
