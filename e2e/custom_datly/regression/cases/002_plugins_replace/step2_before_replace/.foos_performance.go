package foosPerformance

import (
	"github.com/viant/datly_ext/checksum"
	"github.com/viant/xdatly/types/core"
	"reflect"
)

var PackageName = "foosperformance"

func init() {
	core.RegisterType(PackageName, "FoosPerformance", reflect.TypeOf(FoosPerformance{}), checksum.GeneratedTime)
}

type FoosPerformance struct {
	Id           int                 `sqlx:"ID,autoincrement,primaryKey"`
	PerfName     *string             `sqlx:"PERF_NAME" json:",omitempty" validate:"omitempty,le(255)"`
	PerfQuantity *int                `sqlx:"PERF_QUANTITY" json:",omitempty"`
	FooId        *int                `sqlx:"FOO_ID,refTable=FOOS,refColumn=ID" json:",omitempty"`
	Has          *FoosPerformanceHas `setMarker:"true" typeName:"FoosPerformanceHas" json:"-"  sqlx:"-" `
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
