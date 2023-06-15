package session

import (
	"context"
	"database/sql"
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

	validator   validator.Validator
	differ      Differ
	dbProviders map[string]func(ctx context.Context) (*sql.DB, error)
	db          map[string]*Manager
	sync.RWMutex
}

func NewSession(sqlService sqlx.Sqlx, stater state.Stater, opts ...Option) *Session {
	ret := &Session{
		sqlService:  sqlService,
		stater:      stater,
		db:          map[string]*Manager{},
		dbProviders: map[string]func(ctx context.Context) (*sql.DB, error){},
	}

	options(opts).Apply(ret)
	return ret
}

func (s *Session) Validator() *validator.Service {
	return validator.New(s.validator)
}

func (s *Session) Differ() *differ.Service {
	return differ.New(&s.differ)
}

func (s *Session) MessageBus() *mbus.Service {
	return nil
}

func (s *Session) Db(opts ...sqlx.Option) *sqlx.Service {
	return sqlx.New(s.sqlService)
}

func (s *Session) Stater() *state.Service {
	return state.New(s.stater)
}
