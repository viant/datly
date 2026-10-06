package engine

import (
	"context"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type queueValidatedDML interface {
	rhandler.QueueContractDML
	ValidateExecutionGuards(context.Context) error
}

// Only an underlying native capability adds these methods to the focused DML
// surface. The ordinary wrapper and DataKey retain their existing method sets.
type queueContractDMLCapability struct {
	dmlCapability
	queue queueValidatedDML
}

func newQueueAwareDMLCapability(service xhandler.DML, guard *mutationGuard) xhandler.DML {
	base := dmlCapability{service: service, guard: guard}
	if queue, ok := service.(queueValidatedDML); ok {
		return queueContractDMLCapability{dmlCapability: base, queue: queue}
	}
	return base
}
func (c queueContractDMLCapability) InsertWithQueueContract(table string, row any, contract rhandler.QueueContract) error {
	if err := c.guard.check("InsertWithQueueContract"); err != nil {
		return err
	}
	return c.queue.InsertWithQueueContract(table, row, contract)
}
func (c queueContractDMLCapability) DeleteWithQueueContract(table string, row any, contract rhandler.QueueContract) error {
	if err := c.guard.check("DeleteWithQueueContract"); err != nil {
		return err
	}
	return c.queue.DeleteWithQueueContract(table, row, contract)
}
func (c queueContractDMLCapability) ValidateExecutionGuards(ctx context.Context) error {
	return c.queue.ValidateExecutionGuards(ctx)
}
