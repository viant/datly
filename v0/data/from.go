package data

import (
	"github.com/viant/datly/v0/data/sql"
)

type From struct {
	sql.Fragment
	Fragments []*sql.Fragment
}
