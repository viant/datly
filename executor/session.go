package executor

import (
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/insert/batcher"
	"github.com/viant/velty/est"
	"reflect"
	"sync"
)

type Session struct {
	Parameters  *Parameters
	View        *view.View
	mux         sync.Mutex
	State       *est.State
	collections map[string]*batcher.Collection
}

func NewSession(selectors *view.Selectors, aView *view.View) (*Session, error) {
	parameters := NewParameters()
	for viewName := range selectors.Index {
		parameters.Add(viewName, selectors.Index[viewName])
	}

	return &Session{
		Parameters:  parameters,
		View:        aView,
		mux:         sync.Mutex{},
		collections: map[string]*batcher.Collection{},
	}, nil
}

func NewParameters() *Parameters {
	return &Parameters{index: map[string]*view.ParamState{}}
}

func (s *Session) Lookup(v *view.View) *view.ParamState {
	s.mux.Lock()
	state, ok := s.Parameters.index[v.Name]
	if !ok {
		state = &view.ParamState{}
		s.Parameters.index[v.Name] = state
	}
	s.mux.Unlock()

	state.Init(v)
	return state
}

func (s *Session) Collection(executable *expand.Executable) *batcher.Collection {
	if collection, ok := s.collections[executable.Table]; ok {
		return collection
	}

	collection := batcher.NewCollection(reflect.TypeOf(executable.Data))
	s.collections[executable.Table] = collection
	return collection
}
