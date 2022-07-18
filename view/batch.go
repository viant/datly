package view

type BatchData struct {
	ColumnName     string
	Parent         int
	ParentReadSize int

	Values      []interface{}
	ValuesBatch []interface{}
}
