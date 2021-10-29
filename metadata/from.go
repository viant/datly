package metadata

import "github.com/viant/datly/metadata/sql"

type From struct {
	sql.Fragment
	Fragments []*sql.Fragment
}
