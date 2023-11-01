package bars_transform

import (
	"github.com/francoispqt/gojay"
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"reflect"
)

var PackageName = "bars_transform"

func init() {
	core.RegisterType(PackageName, "Bars", reflect.TypeOf(Bars{}), checksum.GeneratedTime)
	core.RegisterType(PackageName, "IntsTransformer", reflect.TypeOf((*IntsTransformer)(nil)).Elem(), checksum.GeneratedTime)
}

type (
	Bars struct {
		Id   int     `sqlx:"ID,primaryKey"`
		Ints *int    `sqlx:"INTS" json:",omitempty"`
		Name *string `sqlx:"NAME" json:",omitempty"`
		Has  *BarHas `setMarker:"true" typeName:"FoosPerformanceHas" json:"-" sqlx:"-"`
	}

	IntsTransformer int
)

type BarHas struct {
	Id   bool
	Name bool
	Ints bool
}

func (b *Bars) Validate() (string, error) {
	return "", nil
}

func (i IntsTransformer) UnmarshalJSONWithOptions(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error {
	var ints []int
	if err := decoder.SliceInt(&ints); err != nil {
		return err
	}

	sum := 0
	for _, value := range ints {
		sum += value
	}

	*dst.(**int) = &sum
	return nil
}
