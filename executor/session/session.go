package session

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/xdatly/handler/differ"
	"github.com/viant/xdatly/handler/mbus"
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
	"sync"
)

type Session struct {
	validator   Validator
	differ      Differ
	dbProviders map[string]func(ctx context.Context) (*sql.DB, error)
	db          map[string]*Manager
	sync.RWMutex
	view     *view.View
	resource *view.Resource
}

func (s *Session) Validator() validator.Service {
	return &s.validator
}

func (s *Session) Differ() differ.Service {
	return &s.differ
}

func (s *Session) MessageBus() mbus.Service {
	return nil
}

func (s *Session) Db(opts ...sqlx.Option) sqlx.Service {
	//TODO fix me
	return &Manager{}
}

func (s *Session) Response() response.Response {
	return nil
}

func (s *Session) StateInto(dest interface{}) error {
	return fmt.Errorf("not yet implemented")
}

func NewSession(opts ...Option) *Session {
	ret := &Session{
		db:          map[string]*Manager{},
		dbProviders: map[string]func(ctx context.Context) (*sql.DB, error){},
	}
	options(opts).Apply(ret)
	return ret
}
