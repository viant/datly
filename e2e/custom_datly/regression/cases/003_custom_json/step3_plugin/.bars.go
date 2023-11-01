package bars

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"reflect"
)

var PackageName = "bars"

func init() {
	core.RegisterType(PackageName, "Bars", reflect.TypeOf(Bars{}), checksum.GeneratedTime)
}

type (
	Bars struct {
		Id   int      `sqlx:"ID,primaryKey"`
		Ints *intsSum `sqlx:"INTS" json:",omitempty"`
		Name *string  `sqlx:"NAME" json:",omitempty"`
		Has  *BarHas  `setMarker:"true" typeName:"FoosPerformanceHas" json:"-" sqlx:"-"`
	}

	intsSum int
)

type BarHas struct {
	Id   bool
	Name bool
	Ints bool
}

func (b *Bars) Validate() (string, error) {
	return "", nil
}

func (i *intsSum) UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	var ints []int
	if err := decoder.SliceInt(&ints); err != nil {
		return err
	}

	sum := intsSum(0)
	for _, value := range ints {
		sum = intsSum(value) + sum
	}

	*dst.(**intsSum) = &sum
	return nil
}
