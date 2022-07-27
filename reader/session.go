package reader

import (
	"fmt"
	"github.com/viant/datly/view"
	"reflect"
	"sync"
)

//Session groups view required to Read view
type (
	Session struct {
		mux       sync.Mutex
		Dest      interface{} //slice
		View      *view.View
		Selectors *view.Selectors
		Parent    *view.View
		Metrics   []*Metric
	}

	Metric struct {
		View      string
		Elapsed   string
		ElapsedMs int
		Rows      int
	}
)

//Init initializes session
func (s *Session) Init() error {
	s.Selectors.Init()
	if _, ok := s.Dest.(*interface{}); !ok {
		viewType := reflect.PtrTo(s.View.Schema.SliceType())
		destType := reflect.TypeOf(s.Dest)
		if viewType.Kind() == reflect.Ptr && destType.Kind() == reflect.Ptr {
			if !viewType.Elem().ConvertibleTo(destType.Elem()) {
				return fmt.Errorf("type mismatch, view slice type is: %v while destination type is %v", viewType.String(), destType.String())
			}
		} else if !viewType.ConvertibleTo(destType) {
			return fmt.Errorf("type mismatch, view slice type is: %v while destination type is %v", viewType.String(), destType.String())
		}
	}

	return nil
}

//AddCriteria adds the supplied view criteria
func (s *Session) AddCriteria(aView *view.View, criteria string, placeholders ...interface{}) {
	sel := s.Selectors.Lookup(aView)
	sel.Criteria = criteria
	sel.Placeholders = placeholders
}

func (s *Session) AddMetric(m *Metric) {
	s.mux.Lock()
	s.Metrics = append(s.Metrics, m)
	s.mux.Unlock()
}

//NewSession creates a session
func NewSession(dest interface{}, aView *view.View) *Session {
	return &Session{
		Dest:      dest,
		View:      aView,
		Selectors: &view.Selectors{Index: make(map[string]*view.Selector)},
	}
}
