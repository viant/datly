package data

import "github.com/viant/datly/data/sql"

type From struct {
	sql.Fragment
	Fragments []*sql.Fragment
}
