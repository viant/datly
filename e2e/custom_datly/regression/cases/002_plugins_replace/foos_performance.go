package foosPerformance

import (
	"github.com/viant/xdatly/types/core"
	"github.com/viant/xdatly/types/custom/checksum"
	"reflect"
)

var PackageName = "foosPerformance"

func init() {
	core.RegisterType(PackageName, "FoosPerformance", reflect.TypeOf(FoosPerformance{}), checksum.GeneratedTime)
}

type FoosPerformance struct {
	Id           int                 `sqlx:"name=ID,primaryKey"`
	PerfName     *string             `sqlx:"name=PERF_NAME" json:",omitempty"`
	PerfQuantity *int                `sqlx:"name=PERF_QUANTITY" json:",omitempty"`
	FooId        *int                `sqlx:"name=FOO_ID" json:",omitempty"`
	Has          *FoosPerformanceHas `setMarker:"true" typeName:"FoosPerformanceHas" json:"-" sqlx:"-"`
}

type FoosPerformanceHas struct {
	Id           bool
	PerfName     bool
	PerfQuantity bool
	FooId        bool
}

func (p *FoosPerformance) Validate() (string, error) {
	return "", nil
}
