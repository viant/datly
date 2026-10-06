package handler

// QueueContract describes one native append, never a whole journal or table.
type QueueContract uint8

const (
	SourceRow QueueContract = iota + 1
	SourceSlice
)

// QueueContractDML is optional Datly-local support. Xdatly DML stays unchanged.
type QueueContractDML interface {
	InsertWithQueueContract(table string, data any, contract QueueContract) error
	DeleteWithQueueContract(table string, data any, contract QueueContract) error
}
