package executor

type StmtIterator interface {
	HasNext() bool
	Next() interface{}
	HasAny() bool
}
