package visitor

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
	"net/http"
)

type (
	//LifecycleVisitor visitor can implement BeforeFetcher and/or AfterFetcher
	LifecycleVisitor interface{}

	//BeforeFetcher represents Lifecycle hook which is called before data was read from the Database.
	BeforeFetcher interface {
		//BeforeFetch one of the lifecycle hooks, returns bool if response was closed (i.e. response.WriteHeader())
		//or just an error if it is needed to stop the routers flow.
		BeforeFetch(response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
	}

	//AfterFetcher represents Lifecycle hook with is called after data was read from Database.
	//It is important to specify View type for assertion type purposes.
	AfterFetcher interface {

		//AfterFetch one of the lifecycle hooks, returns bool if response was closed (i.e. response.WriteHeader())
		//or just an error if it is needed to stop the routers flow.
		//data is type of *[]T or *[]*T
		AfterFetch(data interface{}, response http.ResponseWriter, request *http.Request) (responseClosed bool, err error)
	}

	//RawTransformer transforms raw parameter value to string
	RawTransformer interface {
		TransformRaw(raw string) (string, error)
	}

	ValueTransformer interface {
		TransformIntoValue(ctx context.Context, raw string) (interface{}, error)
	}

	Visitor struct {
		shared.Reference
		Name     string
		_visitor LifecycleVisitor
	}
)

func (v *Visitor) Visitor() LifecycleVisitor {
	return v._visitor
}

func New(name string, visitor LifecycleVisitor) *Visitor {
	return &Visitor{
		Name:     name,
		_visitor: visitor,
	}
}

type Visitors map[string]*Visitor

func (v Visitors) Lookup(name string) (*Visitor, error) {
	visitor, ok := v[name]
	if !ok {
		return nil, fmt.Errorf("not found visitor with name %v", name)
	}

	return visitor, nil
}

func (v Visitors) Register(visitor *Visitor) {
	v[visitor.Name] = visitor
}

func NewVisitors(visitors ...*Visitor) Visitors {
	result := Visitors{}
	for i := range visitors {
		result.Register(visitors[i])
	}

	return result
}

func (v *Visitor) Inherit(visitor *Visitor) {
	v._visitor = visitor._visitor
}
