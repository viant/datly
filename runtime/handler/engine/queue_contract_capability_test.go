package engine

import (
	"context"
	"errors"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

type qcPlainService struct {
	xhandler.DML
	calls int
}

func (d *qcPlainService) Insert(string, any) error { d.calls++; return nil }

type qcOnlyService struct{ *qcPlainService }

func (d *qcOnlyService) InsertWithQueueContract(string, any, rhandler.QueueContract) error {
	d.calls++
	return nil
}
func (d *qcOnlyService) DeleteWithQueueContract(string, any, rhandler.QueueContract) error {
	d.calls++
	return nil
}

type qcNativeService struct {
	*qcOnlyService
	checks int
}

func (d *qcNativeService) ValidateExecutionGuards(context.Context) error { d.checks++; return nil }
func TestQueueContractOptionalEngineCapabilityAndMutationGate(t *testing.T) {
	plain := &qcPlainService{}
	for _, service := range []xhandler.DML{plain, &qcOnlyService{qcPlainService: plain}} {
		capability := newQueueAwareDMLCapability(service, nil)
		if _, ok := capability.(rhandler.QueueContractDML); ok {
			t.Fatal("unsupported service advertised queue contract")
		}
		if err := capability.Insert("records", struct{}{}); err != nil {
			t.Fatal(err)
		}
	}
	if plain.calls != 2 {
		t.Fatal("default DML changed", plain.calls)
	}
	native := &qcNativeService{qcOnlyService: &qcOnlyService{qcPlainService: &qcPlainService{}}}
	guard := &mutationGuard{depth: 1}
	c := newQueueAwareDMLCapability(native, guard).(queueContractDMLCapability)
	for _, call := range []func() error{func() error { return c.InsertWithQueueContract("records", struct{}{}, rhandler.SourceRow) }, func() error { return c.DeleteWithQueueContract("records", struct{}{}, rhandler.SourceRow) }} {
		if err := call(); !errors.Is(err, ErrWriteEligibilityMutation) {
			t.Fatal(err)
		}
	}
	if native.calls != 0 {
		t.Fatal("optional queue writes escaped mutation gate")
	}
	guard.depth = 0
	if err := c.InsertWithQueueContract("records", struct{}{}, rhandler.SourceRow); err != nil || native.calls != 1 {
		t.Fatal(err, native.calls)
	}
	if err := c.ValidateExecutionGuards(context.Background()); err != nil || native.checks != 1 {
		t.Fatal(err, native.checks)
	}
}
