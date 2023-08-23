package reader

import (
	"fmt"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
	"reflect"
	"strings"
	"sync"
)

// Session groups view required to Read view
type (
	Session struct {
		mux           sync.Mutex
		CacheDisabled bool
		Dest          interface{} //slice
		View          *view.View
		States        *view.States
		Parent        *view.View
		Metrics       []*Metric
		ViewMeta      interface{}
		Stats         []*Info
		IncludeSQL    bool
	}

	ParentData struct {
		View     *view.View
		Selector *view.State
	}

	Metric struct {
		View      string
		Elapsed   string
		ElapsedMs int
		Rows      int
	}

	Info struct {
		View         string
		Template     []*Stats `json:",omitempty"`
		TemplateMeta []*Stats `json:",omitempty"`
		Elapsed      string   `json:",omitempty"`
	}

	Stats struct {
		SQL        string        `json:",omitempty"`
		Args       []interface{} `json:",omitempty"`
		CacheStats *cache.Stats  `json:",omitempty"`
		Error      string        `json:",omitempty"`
		CacheError string        `json:",omitempty"`
	}
)

func (s *Info) Name() string {
	return strings.Title(strings.ReplaceAll(s.View, "#", ""))
}

// Init initializes session
func (s *Session) Init() error {
	s.States.Init(s.View)
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

// AddCriteria adds the supplied view criteria
func (s *Session) AddCriteria(aView *view.View, criteria string, placeholders ...interface{}) {
	sel := s.States.Lookup(aView)
	sel.Criteria = criteria
	sel.Placeholders = placeholders
}

func (s *Session) AddMetric(m *Metric) {
	s.mux.Lock()
	s.Metrics = append(s.Metrics, m)
	s.mux.Unlock()
}

// NewSession creates a session
func NewSession(dest interface{}, aView *view.View, opts ...Option) (*Session, error) {
	ret := &Session{
		Dest: dest,
		View: aView,
	}
	err := options(opts).Apply(ret)
	return ret, err
}

func (s *Session) HandleViewMeta(meta interface{}) error {
	s.ViewMeta = meta
	return nil
}

func (s *Session) ParentData() (*ParentData, bool) {
	if s.Parent == nil {
		return nil, false
	}
	return &ParentData{
		View:     s.Parent,
		Selector: s.States.Lookup(s.Parent),
	}, true
}

func (s *Session) AddInfo(info *Info) {
	s.mux.Lock()
	s.Stats = append(s.Stats, info)
	s.mux.Unlock()
}

func (d *ParentData) AsParam() *expand.MetaParam {
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
	sel := s.States.Lookup(v)
	sel.Init(v)
	s.mux.Unlock()
	return sel.State
}
