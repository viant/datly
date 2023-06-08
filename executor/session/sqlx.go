package session

import (
	"context"
	"database/sql"
	"github.com/viant/xdatly/handler/sqlx"
)

type Manager struct {
	sqlx.Service //remove that embedding once all the interface functions are implemented
	tx           *sql.Tx
	db           *sql.DB
	dbProvider   func(ctx context.Context) (*sql.DB, error)
}
