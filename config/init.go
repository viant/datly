package config

import (
	"github.com/viant/xdatly/types/core"
	_ "github.com/viant/xdatly/types/custom/imports"
	"reflect"
	"time"
)

func init() {
	types, _ := core.Types(func(packageName, typeName string, rType reflect.Type, insertedAt time.Time) {
		Config.AddType(packageName, typeName, rType)
	})

	Config.OverridePackageNamedTypes(types)
}
