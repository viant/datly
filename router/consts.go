package router

type QueryParam string

const (
	Fields   QueryParam = "_fields"
	Offset   QueryParam = "_offset"
	OrderBy  QueryParam = "_order_by"
	Limit    QueryParam = "_limit"
	Criteria QueryParam = "_criteria"
)
