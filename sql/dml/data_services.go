package dml

import (
	"context"
	"database/sql"

	sqlconfig "github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/io/delete"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

func (d *Data) serviceOptions(ctx context.Context, db *sql.DB) ([]option.Option, error) {
	dialect, err := d.dialectFor(ctx, db)
	if err != nil {
		return nil, err
	}
	if dialect == nil {
		return nil, nil
	}
	return []option.Option{dialect}, nil
}

func (d *Data) dialectFor(ctx context.Context, db *sql.DB) (*info.Dialect, error) {
	if d == nil {
		return nil, nil
	}
	if d.dialect != nil {
		return d.dialect, nil
	}
	owner := d.owner()
	owner.mu.Lock()
	tx := owner.tx
	owner.mu.Unlock()
	var options []option.Option
	if tx != nil {
		options = append(options, tx)
	}
	dialect, err := sqlconfig.Dialect(ctx, db, options...)
	if err != nil {
		return nil, err
	}
	d.dialect = dialect
	return dialect, nil
}

func (d *Data) inserter(ctx context.Context, db *sql.DB, table string) (*insert.Service, error) {
	if d.insertServices == nil {
		d.insertServices = map[string]*insert.Service{}
	}
	service, ok := d.insertServices[table]
	if ok {
		return service, nil
	}
	var err error
	options, err := d.serviceOptions(ctx, db)
	if err != nil {
		return nil, err
	}
	if strategy := d.owner().sequenceStrategy; strategy != "" {
		options = append(options, strategy)
	}
	service, err = insert.New(ctx, db, table, options...)
	if err != nil {
		return nil, err
	}
	d.insertServices[table] = service
	return service, nil
}

func (d *Data) updater(ctx context.Context, db *sql.DB, table string) (*update.Service, error) {
	if d.updateServices == nil {
		d.updateServices = map[string]*update.Service{}
	}
	service, ok := d.updateServices[table]
	if ok {
		return service, nil
	}
	var err error
	options, err := d.serviceOptions(ctx, db)
	if err != nil {
		return nil, err
	}
	service, err = update.New(ctx, db, table, options...)
	if err != nil {
		return nil, err
	}
	d.updateServices[table] = service
	return service, nil
}

func (d *Data) deleter(ctx context.Context, db *sql.DB, table string) (*delete.Service, error) {
	if d.deleteServices == nil {
		d.deleteServices = map[string]*delete.Service{}
	}
	service, ok := d.deleteServices[table]
	if ok {
		return service, nil
	}
	var err error
	options, err := d.serviceOptions(ctx, db)
	if err != nil {
		return nil, err
	}
	service, err = delete.New(ctx, db, table, options...)
	if err != nil {
		return nil, err
	}
	d.deleteServices[table] = service
	return service, nil
}
