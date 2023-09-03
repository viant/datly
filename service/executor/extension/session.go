package extension

import (
	"context"
	"github.com/viant/xdatly/handler"
	async2 "github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/differ"
	"github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/mbus"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/state"
	"github.com/viant/xdatly/handler/validator"
	"sync"
)

type (
	Session struct {
		sqlService SqlServiceFn
		stater     state.Stater
		validator  *validator.Service
		differ     *differ.Service
		mbus       *mbus.Service
		sync.RWMutex
		templateFlusher TemplateFlushFn
		redirect        RedirectFn
		async           AsyncFn
		http            HttpFn
	}

	SqlServiceFn    func(options *sqlx.Options) (sqlx.Sqlx, error)
	TemplateFlushFn func(ctx context.Context) error
	RedirectFn      func(ctx context.Context, route *http.Route) (handler.Session, error)
	RouterFn        func(ctx context.Context, route *http.Route) (handler.Session, error)
	HttpFn          func() http.Http
	AsyncFn         func() async2.Async
)

func (s *Session) Route(ctx context.Context, route *http.Route) (handler.Session, error) {
	return s.redirect(ctx, route)
}

func (s *Session) Http() http.Http {
	return s.http()
}

func (s *Session) Async() async2.Async {
	return s.async()
}

func NewSession(opts ...Option) *Session {
	ret := &Session{
		mbus:      NewMBus(nil), //TODO pass view message busses
		validator: NewValidator(),
		differ:    NewDiffer(),
	}

	options(opts).Apply(ret)
	return ret
}

func (s *Session) Validator() *validator.Service {
	return validator.New(s.validator)
}

func (s *Session) Redirect(ctx context.Context, route *http.Route) (handler.Session, error) {
	return s.redirect(ctx, route)
}

func (s *Session) Differ() *differ.Service {
	return differ.New(s.differ)
}

func (s *Session) MessageBus() *mbus.Service {
	return s.mbus
}

func (s *Session) Db(opts ...sqlx.Option) (*sqlx.Service, error) {
	sqlxOptions := &sqlx.Options{}
	for _, opt := range opts {
		opt(sqlxOptions)
	}
	service, err := s.sqlService(sqlxOptions)
	if err != nil {
		return nil, err
	}
	return sqlx.New(service), nil
}

func (s *Session) Stater() *state.Service {
	return state.New(s.stater)
}

func (s *Session) FlushTemplate(ctx context.Context) error {
	if s.templateFlusher != nil {
		return s.templateFlusher(ctx)
	}

	return nil
}

func WithSql(sql SqlServiceFn) Option {
	return func(s *Session) {
		s.sqlService = sql
	}
}

func WithRedirect(fn RedirectFn) Option {
	return func(s *Session) {
		s.redirect = fn
	}
}

func WithTemplateFlush(fn TemplateFlushFn) Option {
	return func(s *Session) {
		s.templateFlusher = fn
	}
}

func WithAsync(async AsyncFn) Option {
	return func(s *Session) {
		s.async = async
	}
}

func WithHttp(aHttp HttpFn) Option {
	return func(s *Session) {
		s.http = aHttp
	}
}

func WithStater(stater state.Stater) Option {
	return func(s *Session) {
		s.stater = stater
	}
}
