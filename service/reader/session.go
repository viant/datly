package reader

import (
	"fmt"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/sqlx"
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
		IncludeSQL    bool
		CacheDisabled bool
		RevealMetric  bool
		CacheRefresh  cache.Refresh
		DryRun        bool
		View          *view.View
		State         *view.State
		Parent        *view.View
		Output
	}

	Output struct {
		Data        interface{}
		DataType    reflect.Type
		DataPtr     interface{} //slice pointer
		DataSummary interface{}
		Metrics     Metrics
		Filters     predicate.Filters //filter used by request
	}

	ParentData struct {
		View     *view.View
		Selector *view.Statelet
	}

	Metric struct {
		View      string             `json:",omitempty"`
		Elapsed   string             `json:",omitempty"`
		ElapsedMs int                `json:",omitempty"`
		Rows      int                `json:",omitempty"`
		Execution *TemplateExecution `json:",omitempty"`
	}

	Metrics []*Metric

	TemplateExecution struct {
		Template     []*SQLExecution `json:",omitempty"`
		TemplateMeta []*SQLExecution `json:",omitempty"`
		Elapsed      string          `json:",omitempty"`
	}

	SQLExecution struct {
		SQL        string        `json:",omitempty"`
		Args       []interface{} `json:",omitempty"`
		CacheStats *cache.Stats  `json:",omitempty"`
		Error      string        `json:",omitempty"`
		CacheError string        `json:",omitempty"`
	}
)

func (m Metrics) Basic() Metrics {
	var result = make(Metrics, len(m))
	copy(result, m)
	for _, item := range m {
		item.Execution = nil
	}
	return result
}

// SQL returns main view SQL
func (m *Metrics) SQL() string {
	if m == nil || len(*m) == 0 {
		return ""
	}
	return (*m)[0].SQL()
}

// SQL returns view SQL
func (m *Metric) SQL() string {
	if m.Execution != nil && len(m.Execution.Template) > 0 {
		tmpl := m.Execution.Template[0]
		SQL := shared.ExpandSQL(tmpl.SQL, tmpl.Args)
		SQL = strings.ReplaceAll(SQL, "\n", "\\n")
		return SQL
	}
	return ""
}

func (e Metrics) Lookup(viewName string) *Metric {
	for _, candidate := range e {
		if candidate.View == viewName {
			return candidate
		}
	}
	return nil
}

func (i *Metric) HideSQL() *Metric {
	ret := *i
	if i.Execution == nil {
		return &ret
	}
	ret.Execution = &TemplateExecution{
		Template:     make([]*SQLExecution, len(i.Execution.Template)),
		TemplateMeta: make([]*SQLExecution, len(i.Execution.TemplateMeta)),
	}
	copy(ret.Execution.Template, i.Execution.Template)
	copy(ret.Execution.TemplateMeta, i.Execution.TemplateMeta)
	for _, elem := range i.Execution.Template {
		elem.SQL = ""
		elem.Args = nil
	}
	for _, elem := range i.Execution.TemplateMeta {
		elem.SQL = ""
		elem.Args = nil
	}
	return &ret
}

func (e Metrics) ParametrizedSQL() []*sqlx.SQL {
	var result []*sqlx.SQL
	for _, metric := range e {
		result = append(result, metric.ParametrizedSQL()...)
	}
	return result
}

func (e *Metric) ParametrizedSQL() []*sqlx.SQL {
	var result = make([]*sqlx.SQL, 0)
	if e.Execution == nil {
		return result
	}
	for _, tmpl := range e.Execution.Template {
		result = append(result, &sqlx.SQL{Query: tmpl.SQL, Args: tmpl.Args})
	}
	return result
}

func (i *Metric) Name() string {
	return strings.Title(i.View)
}

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

func (s *Session) AddMetric(m *Metric) {
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

// NewSession creates a session
func NewSession(dest interface{}, aView *view.View, opts ...Option) (*Session, error) {
	ret := &Session{
		Output: Output{DataPtr: dest},
		View:   aView,
	}

	err := options(opts).Apply(ret)
	if ret.State == nil {
		ret.State = view.NewState()
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
	sel := s.State.Lookup(v)
	sel.Init(v)
	s.mux.Unlock()
	return sel.Template
}
