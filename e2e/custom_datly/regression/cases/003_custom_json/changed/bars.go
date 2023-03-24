package changed

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"reflect"
)

var PackageName = "bars"

func init() {
	core.RegisterType(PackageName, "Bar", reflect.TypeOf(Bar{}), checksum.GeneratedTime)
}

type (
	Bar struct {
		Id   int      `sqlx:"name=ID,primaryKey"`
		Ints *intsSum `sqlx:"name=INTS" json:",omitempty"`
		Name *string  `sqlx:"NAME" json:",omitempty"`
		Has  *BarHas  `presenceIndex:"true" typeName:"FoosPerformanceHas" json:"-" sqlx:"presence=true"`
	}

	intsSum int
)

type BarHas struct {
	Id   bool
	Name bool
	Ints bool
}

func (b *Bar) Validate() (string, error) {
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
