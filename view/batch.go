package view

type BatchData struct {
	ColumnName     string
	Parent         int
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
