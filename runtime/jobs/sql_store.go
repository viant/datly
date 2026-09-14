package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/info"
	xasync "github.com/viant/xdatly/async"
	"reflect"
	"strings"
	"sync"
	"time"
)

var ErrNotFound = errors.New("job not found")
var ErrTransition = errors.New("job state transition rejected")

// SQLStore uses the original native SQLX insert/update and typed read owners.
// Its database is the configured job connector, never the application Data scope.
type SQLStore struct {
	admission   sync.Mutex
	db          *sql.DB
	table       string
	projection  string
	dialect     *info.Dialect
	inserter    *insert.Service
	completer   *update.Service
	deactivator *update.Service
}

func NewSQLStore(ctx context.Context, config SQLConfig) (*SQLStore, error) {
	db, resolvedDialect, table, err := config.resolve(ctx)
	if err != nil {
		return nil, err
	}
	if !config.DisableTableCreation {
		if err := config.ensureTable(ctx, db, resolvedDialect); err != nil {
			return nil, fmt.Errorf("provision original job table: %w", err)
		}
	}
	// The qualified identifier already carries its dialect delimiters; prevent
	// the insert builder from quoting the entire qualified path again.
	dialect := *resolvedDialect
	dialect.SpecialKeywordEscapeQuote = 0
	inserter, err := insert.New(ctx, db, table, &dialect)
	if err != nil {
		return nil, err
	}
	completer, err := update.New(ctx, db, table, &dialect)
	if err != nil {
		return nil, err
	}
	deactivator, err := update.New(ctx, db, table, &dialect)
	if err != nil {
		return nil, err
	}
	columns, err := sqlxio.StructColumns(reflect.TypeOf(Record{}), "sqlx")
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		name := column.Name()
		if name == "Deactivated" {
			name = "COALESCE(Deactivated, FALSE) AS Deactivated"
		}
		names = append(names, name)
	}
	result := &SQLStore{db: db, table: table, projection: strings.Join(names, ", "), dialect: &dialect, inserter: inserter, completer: completer, deactivator: deactivator}
	// Fail at configuration time if the configured table is missing or unreadable.
	_, err = result.read(ctx, "WHERE 1 = 0")
	if err != nil {
		return nil, fmt.Errorf("initialize original job table (DisableTableCreation=%t): %w", config.DisableTableCreation, err)
	}
	return result, nil
}
func (s *SQLStore) Create(ctx context.Context, r *Record) error {
	if err := r.validate(); err != nil {
		return err
	}
	count, _, err := s.inserter.Exec(ctx, r)
	return s.changed(count, err)
}
func (s *SQLStore) Get(ctx context.Context, id string) (*Record, error) {
	records, err := s.read(ctx, "WHERE ID = ?", id)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, ErrNotFound
	}
	if len(records) != 1 {
		return nil, fmt.Errorf("job ID %s is not unique", id)
	}
	return records[0], nil
}
func (s *SQLStore) read(ctx context.Context, query string, args ...any) ([]*Record, error) {
	reader, err := sqlxread.New(ctx, s.db, s.dialect.EnsurePlaceholders("SELECT "+s.projection+" FROM "+s.table+" "+query), func() any { return &Record{} })
	if err != nil {
		return nil, err
	}
	defer func() {
		if statement := reader.Stmt(); statement != nil {
			_ = statement.Close()
		}
	}()
	var result []*Record
	err = reader.QueryAll(ctx, func(row any) error { result = append(result, row.(*Record)); return nil }, args...)
	return result, err
}
func (s *SQLStore) changed(count int64, err error) error {
	if err != nil {
		return err
	}
	if count != 1 {
		return ErrTransition
	}
	return nil
}

func (s *SQLStore) match(ctx context.Context, owner *Record, ttl, errorTTL time.Duration) (*Record, error) {
	now := time.Now().UTC()
	rows, err := s.read(ctx, "WHERE MatchKey = ? AND Method = ? AND URI = ? AND COALESCE(Deactivated, FALSE) = FALSE AND ((CreationTime >= ? AND Status <> ?) OR (CreationTime >= ? AND Status = ?)) ORDER BY CreationTime DESC", owner.MatchKey, owner.Method, owner.URI, now.Add(-ttl), xasync.StatusError, now.Add(-errorTTL), xasync.StatusError)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	if len(rows) > 1 && rows[0].CreationTime.Equal(rows[1].CreationTime) {
		return nil, fmt.Errorf("ambiguous durable job match key")
	}
	return rows[0], nil
}
