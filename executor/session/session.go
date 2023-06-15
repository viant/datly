package session

import (
	"github.com/viant/xdatly/handler/differ"
	"github.com/viant/xdatly/handler/mbus"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/state"
	"github.com/viant/xdatly/handler/validator"
	"sync"
)

type Session struct {
	sqlService sqlx.Sqlx
	stater     state.Stater
	validator  *validator.Service
	differ     *differ.Service
	mbus       *mbus.Service
	sync.RWMutex
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

func (s *Session) Differ() *differ.Service {
	return differ.New(s.differ)
}

func (s *Session) MessageBus() *mbus.Service {
	return s.mbus
}

func (s *Session) Db(opts ...sqlx.Option) *sqlx.Service {
	return sqlx.New(s.sqlService)
}

func (s *Session) Stater() *state.Service {
	return state.New(s.stater)
}

func WithStater(stater state.Stater) Option {
	return func(s *Session) {
		s.stater = state.New(stater)
	}
}

func WithSql(sql sqlx.Sqlx) Option {
	return func(s *Session) {
		s.sqlService = sql
	}
}
