package changed

import (
	"fmt"
	"github.com/viant/datly/xregistry/types/core"
	"github.com/viant/datly/xregistry/types/custom/generated"
	"reflect"
)

var PackageName = "foosPerformance"

func init() {
	core.RegisterType(PackageName, "FoosPerformance", reflect.TypeOf(FoosPerformance{}), generated.GeneratedTime)
}

type FoosPerformance struct {
	Id           int                 `sqlx:"name=ID,primaryKey"`
	PerfName     *string             `sqlx:"name=PERF_NAME" json:",omitempty"`
	PerfQuantity *int                `sqlx:"name=PERF_QUANTITY" json:",omitempty"`
	FooId        *int                `sqlx:"name=FOO_ID" json:",omitempty"`
	Has          *FoosPerformanceHas `presenceIndex:"true" typeName:"FoosPerformanceHas" json:"-" sqlx:"presence=true"`
}

type FoosPerformanceHas struct {
	Id           bool
	PerfName     bool
	PerfQuantity bool
	FooId        bool
}

func (p *FoosPerformance) Validate() (string, error) {
	return "", fmt.Errorf("can't insert FoosPerformance")
}
