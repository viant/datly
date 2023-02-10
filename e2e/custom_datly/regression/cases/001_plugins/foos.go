package foos

import (
	"fmt"
	"github.com/viant/datly/xregistry/types/core"
	"github.com/viant/datly/xregistry/types/custom/generated"
	"reflect"
)

var PackageName = "foos"

func init() {
	core.RegisterType(PackageName, "Foos", reflect.TypeOf(Foos{}), generated.GeneratedTime)
}

type Foos struct {
	Id       int      `sqlx:"name=ID,primaryKey"`
	Name     *string  `sqlx:"name=NAME" json:",omitempty"`
	Quantity *int     `sqlx:"name=QUANTITY" json:",omitempty"`
	Has      *FoosHas `presenceIndex:"true" typeName:"FoosHas" json:"-" sqlx:"presence=true"`
}

type FoosHas struct {
	Id       bool
	Name     bool
	Quantity bool
}

func (f *Foos) Validate() (string, error) {
	if f.Id == 0 {
		return "", fmt.Errorf("invalid ID")
	}

	return "", nil
}
