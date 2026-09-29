package composelinked

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/viant/datly/report/cubecompose"
	"github.com/viant/xdatly/predicate"
)

type PredicateInput interface {
	DatlyTenant() string
	DatlyChannel() string
}

type ScopePredicate struct {
	Input PredicateInput `bind:"kind=input,required"`
}

// Retain the concrete pointer type for native linked-type discovery. An unused
// handler value and a no-op init can be eliminated by the Go linker.
var LinkedTypes = []reflect.Type{reflect.TypeFor[*ScopePredicate]()}

type FrameObservation struct {
	Frame           cubecompose.FrameContext
	Channel, Tenant string
	Deadline        time.Time
	Err             error
}

var Observations struct {
	sync.Mutex
	Values  []FrameObservation
	Entered chan struct{}
}

func (p *ScopePredicate) Compute(ctx context.Context, value any) (*predicate.Criteria, error) {
	if p.Input == nil || value != p.Input.DatlyChannel() {
		return nil, fmt.Errorf("missing canonical generated input")
	}
	frame, ok := cubecompose.FrameContextFrom(ctx)
	if !ok {
		return &predicate.Criteria{Expression: "s.channel = ?", Placeholders: []any{p.Input.DatlyChannel()}}, nil
	}
	if frame.Snapshot.IsZero() {
		return nil, fmt.Errorf("missing frame or canonical generated input")
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, fmt.Errorf("compose deadline missing")
	}
	event := FrameObservation{Frame: frame, Channel: p.Input.DatlyChannel(), Tenant: p.Input.DatlyTenant(), Deadline: deadline}
	Observations.Lock()
	entered := Observations.Entered
	Observations.Unlock()
	if p.Input.DatlyChannel() == "wait" {
		if entered != nil {
			entered <- struct{}{}
		}
		<-ctx.Done()
		event.Err = ctx.Err()
	}
	Observations.Lock()
	Observations.Values = append(Observations.Values, event)
	Observations.Unlock()
	if event.Err != nil {
		return nil, event.Err
	}
	return &predicate.Criteria{Expression: "s.channel = ?", Placeholders: []any{p.Input.DatlyChannel()}}, nil
}
