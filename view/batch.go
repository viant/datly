package view

type BatchData struct {
	ColumnNames    []string
	Size           int
	ParentReadSize int

	Values               []interface{}   // all scalar values from parent
	ValuesBatch          []interface{}   // batched scalar values
	CompositeValues      [][]interface{} // all composite parent tuples
	CompositeValuesBatch [][]interface{} // batched composite tuples
}

func (b *BatchData) ColIn() []interface{} {
	return b.Values
}

func (b *BatchData) ColInBatch() []interface{} {
	return b.ValuesBatch
}

func (b *BatchData) HasComposite() bool {
	return b != nil && len(b.CompositeValues) > 0
}

func (b *BatchData) CompositeIn() [][]interface{} {
	if b == nil {
		return nil
	}
	return b.CompositeValues
}

func (b *BatchData) CompositeInBatch() [][]interface{} {
	if b == nil {
		return nil
	}
	return b.CompositeValuesBatch
}
