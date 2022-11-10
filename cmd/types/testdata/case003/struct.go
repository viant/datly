package case001

import (
	"github.com/viant/datly/cmd/types/testdata/case003/dep"
)

type Foo struct {
	Name  string
	ID    int
	Price float64
	Boo   dep.Boo
}
