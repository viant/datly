package executor

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/option"
	"sync"
)

type sqlxIO struct {
	update map[string]*update.Service
	insert map[string]*insert.Service
	sync.RWMutex
}

func (s *sqlxIO) Updater(ctx context.Context, db *sql.DB, table string, options ...option.Option) (*update.Service, error) {
	s.RWMutex.RLock()
	service, ok := s.update[table]
	if ok {
		return service, nil
	}
	s.RWMutex.RUnlock()
	var err error
	service, err = update.New(ctx, db, table, options...)
	if err == nil {
		s.RWMutex.Lock()
		s.update[table] = service
		s.RWMutex.Unlock()
	}
	return service, err
}

func (s *sqlxIO) Inserter(ctx context.Context, db *sql.DB, table string, options ...option.Option) (*insert.Service, error) {
	s.RWMutex.RLock()
	service, ok := s.insert[table]
	if ok {
		return service, nil
	}
	s.RWMutex.RUnlock()
	var err error
	service, err = insert.New(ctx, db, table, options...)
	if err == nil {
		s.RWMutex.Lock()
		s.insert[table] = service
		s.RWMutex.Unlock()
	}
	return service, err
}

func newSqlxIO() *sqlxIO {
	return &sqlxIO{
		update: map[string]*update.Service{},
		insert: map[string]*insert.Service{},
	}
}
