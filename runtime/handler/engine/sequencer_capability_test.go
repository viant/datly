package engine

import (
	"context"
	"errors"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

type allocationProbe struct{ calls int }

func (p *allocationProbe) Allocate(context.Context, string, any, string) error { p.calls++; return nil }

type reservationProbe struct {
	allocationProbe
	ctx             context.Context
	table, selector string
	value           any
	err             error
}

func (p *reservationProbe) Reserve(ctx context.Context, table string, value any, selector string) error {
	p.ctx, p.table, p.value, p.selector = ctx, table, value, selector
	return p.err
}

func TestSequencerCapabilityForwardsReservation(t *testing.T) {
	ctx := context.Background()
	value := &struct{ ID int }{6}
	failure := errors.New("reservation failure")
	service := &reservationProbe{err: failure}
	var focused xhandler.Sequencer = sequencerCapability{service: service}
	reserver := focused.(interface {
		Reserve(context.Context, string, any, string) error
	})
	if err := reserver.Reserve(ctx, "records", value, "ID"); err != failure {
		t.Fatalf("error=%v", err)
	}
	if service.ctx != ctx || service.table != "records" || service.value != value || service.selector != "ID" || service.calls != 0 {
		t.Fatal("reservation arguments or allocation changed")
	}
	custom := &allocationProbe{}
	compatible := sequencerCapability{service: custom}
	if err := compatible.Reserve(ctx, "records", value, "ID"); err != nil {
		t.Fatal(err)
	}
	if err := compatible.Allocate(ctx, "records", value, "ID"); err != nil || custom.calls != 1 {
		t.Fatalf("custom allocation contract changed: %v", err)
	}
}
