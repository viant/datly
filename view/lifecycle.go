package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"net/http"
	"reflect"
)

type (
	CodecFactory interface {
		New(codec *Codec, paramType reflect.Type) (Valuer, error)
	}

	TypeProvider interface {
		ResultType() reflect.Type
	}

	Valuer interface {
		Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error)
	}

	CodecDef interface {
		LifecycleVisitor
		TypeProvider
	}

	//LifecycleVisitor visitor can implement BeforeFetcher and/or AfterFetcher
	LifecycleVisitor interface {
		Valuer() Valuer
		Name() string
	}

	//BeforeFetcher represents Lifecycle hook which is called before view was read from the Database.
	BeforeFetcher interface {
		//BeforeFetch one of the lifecycle hooks, returns bool if response was closed (i.e. response.WriteHeader())
		//or just an error if it is needed to stop the routers flow.
		BeforeFetch(response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
	}

	//AfterFetcher represents Lifecycle hook with is called after view was read from Database.
	//It is important to specify View type for assertion type purposes.
	AfterFetcher interface {

		//AfterFetch one of the lifecycle hooks, returns bool if response was closed (i.e. response.WriteHeader())
		//or just an error if it is needed to stop the routers flow.
		//view is type of *[]T or *[]*T
		AfterFetch(data interface{}, response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
	}

	Visitor struct {
		shared.Reference
		name     string
		_visitor Valuer
	}
)

func (v *Visitor) Valuer() Valuer {
	return v._visitor
}

func (v *Visitor) Name() string {
	return v.name
}

func (v *Visitor) Visitor() interface{} {
	return v._visitor
}

func NewVisitor(name string, visitor Valuer) LifecycleVisitor {
	return &Visitor{
		name:     name,
		_visitor: visitor,
	}
}

type Visitors map[string]LifecycleVisitor

func (v Visitors) Lookup(name string) (LifecycleVisitor, error) {
	visitor, ok := v[name]
	if !ok {
		return nil, fmt.Errorf("not found visitor with name %v", name)
	}

	return visitor, nil
}

func (v Visitors) Register(visitor LifecycleVisitor) {
	v[visitor.Name()] = visitor
}

func NewCodecs(visitors ...LifecycleVisitor) Visitors {
	result := Visitors{}
	for i := range visitors {
		result.Register(visitors[i])
	}

	return result
}

func (v *Visitor) Inherit(visitor LifecycleVisitor) {
	v._visitor = visitor.Valuer()
}

type valuer struct {
	fn func(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error)
}

func (v *valuer) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	return v.fn(ctx, raw, options...)
}

type (
	codec struct {
		name       string
		visitor    Valuer
		resultType reflect.Type
	}
)

func (c *codec) Name() string {
	return c.name
}

func (c *codec) Valuer() Valuer {
	return c.visitor
}

func (c *codec) ResultType() reflect.Type {
	return c.resultType
}

func NewCodec(name string, valuer Valuer, resultType reflect.Type) CodecDef {
	return &codec{
		name:       name,
		visitor:    valuer,
		resultType: resultType,
	}
}
