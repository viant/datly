package handler

import (
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
)

// CriteriaDML is a Datly-owned optional data capability. The public SDK does not
// depend on SQLX; predicate evaluation remains in the existing Datly compiler.
type CriteriaDML interface {
	UpdateWithCriteria(string, any, *sqlx.Criteria, ...xhandler.Option) error
	DeleteWithCriteria(string, any, *sqlx.Criteria, ...xhandler.Option) error
}
