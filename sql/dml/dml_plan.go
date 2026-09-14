package dml

import "reflect"

const defaultInsertBatchSize = 100

type executionStep struct {
	kind       dataOperationKind
	table      string
	operations []*dataOperation
}

func buildExecutionPlan(operations []*dataOperation) []executionStep {
	if len(operations) == 0 {
		return nil
	}
	result := make([]executionStep, 0, len(operations))
	for _, operation := range operations {
		if len(result) > 0 && canExtendInsertStep(result[len(result)-1], operation) {
			last := &result[len(result)-1]
			last.operations = append(last.operations, operation)
			continue
		}
		result = append(result, executionStep{
			kind:       operation.kind,
			table:      operation.table,
			operations: []*dataOperation{operation},
		})
	}
	return result
}

func canExtendInsertStep(step executionStep, operation *dataOperation) bool {
	if step.kind != dataOpInsert || operation.kind != dataOpInsert {
		return false
	}
	if step.table != operation.table || len(step.operations) == 0 {
		return false
	}
	currentType := reflect.TypeOf(step.operations[0].data)
	nextType := reflect.TypeOf(operation.data)
	return currentType != nil && currentType == nextType
}

func boundedInsertBatchSize(size int) int {
	if size <= 0 {
		return 0
	}
	if size < defaultInsertBatchSize {
		return size
	}
	return defaultInsertBatchSize
}
