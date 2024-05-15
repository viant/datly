package view

type BatchData struct {
	ColumnNames    []string
	Size           int
	ParentReadSize int

	Values      []interface{}
	ValuesBatch []interface{}
}

func (b *BatchData) ColIn() []interface{} {
	return b.Values
}

func (b *BatchData) ColInBatch() []interface{} {
	return b.ValuesBatch
}
