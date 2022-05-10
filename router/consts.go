package router

type QueryParam string

const (
	Fields   QueryParam = "_fields"
	Offset   QueryParam = "_offset"
	OrderBy  QueryParam = "_orderby"
	Limit    QueryParam = "_limit"
	Criteria QueryParam = "_criteria"
)
