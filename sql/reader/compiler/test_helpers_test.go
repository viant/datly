package compiler

import (
	"io/fs"
	"reflect"

	"github.com/viant/bindly"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	readerpredicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	sqlreader "github.com/viant/datly/sql/reader"
	xcodec "github.com/viant/xdatly/codec"
)

type ArtifactInput struct {
	Component       *spec.Component
	InputType       reflect.Type
	OutputType      reflect.Type
	CodecFactory    xcodec.Factory
	TypeLookup      func(name string) (reflect.Type, error)
	DirectViewField string
	Resources       fs.FS
}

type Artifact struct{ Reader *sqlreader.Plan }

func BuildArtifact(input ArtifactInput) (*Artifact, error) {
	bindings, err := handlercompiler.New(handlercompiler.Input{
		Component: input.Component, InputType: input.InputType,
		CodecFactory: input.CodecFactory, Resources: input.Resources,
	}).CompileBindings()
	if err != nil {
		return nil, err
	}
	predicate, err := readerpredicate.Compile(readerpredicate.CompileInput{
		Component: input.Component, InputType: input.InputType, Bindings: bindings,
		Lookup: readerpredicate.LookupType(input.TypeLookup),
	})
	if err != nil {
		return nil, err
	}
	plan, err := Compile(Input{
		CodecFactory: input.CodecFactory,
		Component:    input.Component, InputType: input.InputType, OutputType: input.OutputType,
		Bindings:  bindings,
		Predicate: predicate, TypeLookup: input.TypeLookup,
		DirectViewField: input.DirectViewField, Resources: input.Resources,
	})
	if err != nil {
		return nil, err
	}
	return &Artifact{Reader: plan}, nil
}

func testBindings(component *spec.Component, inputType reflect.Type) ([]bindly.BindingSpec, error) {
	return handlercompiler.BuildBindingSpecs(component, inputType, nil)
}
