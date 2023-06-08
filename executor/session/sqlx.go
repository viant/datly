package session

import (
	"context"
	"database/sql"
	"github.com/viant/xdatly/handler/sqlx"
	"github.com/viant/xdatly/handler/validator"
)

type Manager struct {
	sqlx.Service //remove that embedding once all the interface functions are implemented
	tx           *sql.Tx
	db           *sql.DB
	dbProvider   func(ctx context.Context) (*sql.DB, error)
}

func (m *Manager) Validator() validator.Service {
	//TO resolve db, maybe change signature to service , error
	return &SqlxValidator{db: m.db}
}
