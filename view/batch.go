package view

type BatchData struct {
	ColumnNames    []string
	Size           int
	ParentReadSize int

	Values      []interface{} //all values from parent
	ValuesBatch []interface{} //batched values defined view.Batch.Size
}

func (b *BatchData) ColIn() []interface{} {
	return b.Values
}

func (b *BatchData) ColInBatch() []interface{} {
	return b.ValuesBatch
}
