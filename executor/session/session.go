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
	validator   Validator
	differ      Differ
	dbProviders map[string]func(ctx context.Context) (*sql.DB, error)
	db          map[string]*Manager
	sync.RWMutex
	mbus mbus.Service
}

func (s *Session) Validator() validator.Service {
	return &s.validator
}

func (s *Session) Stater() state.Stater {
	return nil
}

func (s *Session) Differ() differ.Service {
	return &s.differ
}

func (s *Session) MessageBus() mbus.Service {
	return s.mbus
}

func (s *Session) Db(opts ...sqlx.Option) sqlx.Service {
	//TODO fix me
	return &Manager{}
}

func NewSession(opts ...Option) *Session {
	ret := &Session{
		mbus:        NewMBus(nil), //TODO pass view message busses
		db:          map[string]*Manager{},
		dbProviders: map[string]func(ctx context.Context) (*sql.DB, error){},
	}
	options(opts).Apply(ret)
	return ret
}
