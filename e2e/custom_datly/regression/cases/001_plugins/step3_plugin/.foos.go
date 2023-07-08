package foos

import (
	"fmt"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"reflect"
)

var PackageName = "foos"

func init() {
	core.RegisterType(PackageName, "Foos", reflect.TypeOf(Foos{}), checksum.GeneratedTime)
}

type Foos struct {
	Id       int      `sqlx:"name=ID,primaryKey"`
	Name     *string  `sqlx:"name=NAME" json:",omitempty"`
	Quantity *int     `sqlx:"name=QUANTITY" json:",omitempty"`
	Has      *FoosHas `setMarker:"true" typeName:"FoosHas" json:"-" sqlx:"-"`
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
