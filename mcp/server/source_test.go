package server

import (
	"context"
	"errors"
	"testing"
)

type testSource struct {
	service    ServerService
	calls      int
	err        error
	nilContext bool
}

func (s *testSource) Pin(ctx context.Context) (context.Context, ServerService, error) {
	s.calls++
	if s.nilContext {
		ctx = nil
	}
	return ctx, s.service, s.err
}

func TestSourcePinsOnceAndSeparatesOwners(t *testing.T) {
	a := &testSource{service: newTransportTestService(nil)}
	b := &testSource{service: newTransportTestService(nil)}
	first, second := &sourceBinding{source: a}, &sourceBinding{source: b}
	ctx, err := first.pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	a.err = errors.New("source unavailable after pin")
	ctx, err = second.pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err = first.pin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	one, _ := first.service(ctx)
	two, _ := second.service(ctx)
	if one != a.service || two != b.service || a.calls != 1 || b.calls != 1 {
		t.Fatal("snapshot owner or single-pin contract lost")
	}
}

func TestSourceRejectsIncompleteSnapshot(t *testing.T) {
	for _, test := range []struct {
		name   string
		source testSource
	}{{name: "error", source: testSource{err: errors.New("private")}}, {name: "missing service"}, {name: "nil context", source: testSource{nilContext: true, service: newTransportTestService(nil)}}} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := (&sourceBinding{source: &test.source}).pin(context.Background()); err == nil {
				t.Fatal("incomplete snapshot accepted")
			}
		})
	}
}
