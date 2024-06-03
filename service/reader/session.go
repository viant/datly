package reader

import (
	"fmt"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
	"github.com/viant/xdatly/handler/response"
	"reflect"
	"sync"
)

// Session groups view required to Read view
type (
	Session struct {
		mux           sync.Mutex
		IncludeSQL    bool
		CacheDisabled bool
		RevealMetric  bool
		CacheRefresh  cache.Refresh
		DryRun        bool
		View          *view.View
		State         *view.State
		Parent        *view.View
		Output
		MetricPtr *response.Metrics
	}

	Output struct {
		Data        interface{}
		DataType    reflect.Type
		DataPtr     interface{} //slice pointer
		DataSummary interface{}
		Metrics     response.Metrics
		Filters     predicate.Filters //filter used by request
	}

	ParentData struct {
		View     *view.View
		Selector *view.Statelet
	}
)

func (r *Output) syncData(cardinality state.Cardinality) {
	if r.DataPtr == nil {
		return
	}
	slice := reflect.ValueOf(r.DataPtr).Elem()
	//if cardinality == state.One {//TODO uncomment is here move to one cardinality handling here
	//	switch slice.Len() {
	//	case 0:
	//		r.Data = nil
	//		return
	//	case 1:
	//		r.Data = slice.Index(0).Interface()
	//		return
	//	}
	//}
	r.Data = slice.Interface()
}

// Init initializes session
func (s *Session) Init() error {
	s.State.Init(s.View)
	if _, ok := s.DataPtr.(*interface{}); !ok {
		viewType := reflect.PtrTo(s.View.Schema.SliceType())
		destType := reflect.TypeOf(s.DataPtr)

		if viewType.Kind() == reflect.Ptr && destType.Kind() == reflect.Ptr {
			if !viewType.Elem().ConvertibleTo(destType.Elem()) {
				return fmt.Errorf("type mismatch, view slice type is: %v while destination type is %v", viewType.String(), destType.String())
			}
		} else if !viewType.ConvertibleTo(destType) {
			return fmt.Errorf("type mismatch, view slice type is: %v while destination type is %v", viewType.String(), destType.String())
		}
		s.DataType = s.View.Schema.SliceType()
	}
	return nil
}

// AddCriteria adds the supplied view criteria
func (s *Session) AddCriteria(aView *view.View, criteria string, placeholders ...interface{}) {
	sel := s.State.Lookup(aView)
	sel.Criteria = criteria
	sel.Placeholders = placeholders
}

func (s *Session) AddMetric(m *response.Metric) {
	s.mux.Lock()
	s.Metrics = append(s.Metrics, m)
	s.mux.Unlock()
}

// WithDryRun returns with dry run option
func WithDryRun() Option {
	return func(session *Session) error {
		session.DryRun = true
		return nil
	}
}

// WithMetrics returns with dry run option
func WithMetrics(metrics *response.Metrics) Option {
	return func(session *Session) error {
		session.MetricPtr = metrics
		return nil
	}
}

// NewSession creates a session
func NewSession(dest interface{}, aView *view.View, opts ...Option) (*Session, error) {
	viewState := view.NewState()
	ret := &Session{
		Output: Output{DataPtr: dest},
		View:   aView,
		State:  viewState,
	}
	err := options(opts).Apply(ret)
	if ret.State == nil {
		ret.State = viewState
	}
	return ret, err
}

func (s *Session) HandleViewMeta(meta interface{}) error {
	s.DataSummary = meta
	return nil
}

func (s *Session) ParentData() (*ParentData, bool) {
	if s.Parent == nil {
		return nil, false
	}
	return &ParentData{
		View:     s.Parent,
		Selector: s.State.Lookup(s.Parent),
	}, true
}

func (d *ParentData) AsParam() *expand.ViewContext {
	if d == nil {
		return nil
	}

	return view.AsViewParam(d.View, d.Selector, nil)
}

func (s *Session) IsCacheEnabled(aView *view.View) bool {
	return !s.CacheDisabled && aView.Cache != nil
}

func (s *Session) Lookup(v *view.View) *structology.State {
	s.mux.Lock()
	sel := s.State.Lookup(v)
	sel.Init(v)
	s.mux.Unlock()
	return sel.Template
}
