package bootstrap

import (
	"io/fs"
	"reflect"

	"github.com/viant/bindly"
	dcontract "github.com/viant/datly/runtime/contract"
	"github.com/viant/datly/runtime/handler/compiler"
	xcodec "github.com/viant/xdatly/codec"
)

// ContractStatePlans compiles arbitrary hook state through the same linked
// metadata and codec owners as component inputs. It retains no invocation data.
// The supplied factory/resources/lookup must belong to the owning generation.
func ContractStatePlans(factory xcodec.Factory, resources fs.FS, lookup func(string) (reflect.Type, error)) *dcontract.StatePlans {
	return dcontract.NewStatePlansWithBindings(func(typ reflect.Type) (*bindly.Plan, []bindly.BindingSpec, error) {
		component, err := (ContractResolver{InputType: linkedContractType(typ)}).Resolve()
		if err != nil {
			return nil, nil, err
		}
		codecs := newCodecFactory(factory)
		codecs.lookup = lookup
		bindings, err := compiler.New(compiler.Input{Component: component, InputType: typ, CodecFactory: codecs, Resources: resources, TypeLookup: lookup}).CompileBindings()
		if err != nil {
			return nil, nil, err
		}
		injector, err := bindly.NewInjector()
		if err != nil {
			return nil, nil, err
		}
		plan, err := injector.CompilePlan(typ, bindings...)
		return plan, bindings, err
	})
}
