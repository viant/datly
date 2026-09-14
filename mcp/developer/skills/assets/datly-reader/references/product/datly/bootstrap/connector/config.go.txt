// Package connector composes explicitly configured database/sql connections.
package connector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	dsql "github.com/viant/datly/sql"
	"github.com/viant/scy"
)

// Config retains original single-connection names, DSN expansion and pool options.
type Config struct {
	Name              string
	Driver            string
	DSN               string
	Secret            *scy.Resource
	MaxIdleConns      int
	MaxOpenConns      int
	ConnMaxIdleTimeMs int
	ConnMaxLifetimeMs int
}

// Set owns only handles opened for this application lifetime. Reader, dialect,
// transaction and row mapping ownership remains with SQLComponent and SQLX.
type Set struct {
	SQL     *dsql.SQLComponent
	handles []*sql.DB
}

func Open(ctx context.Context, configs []Config, defaultName string) (_ *Set, err error) {
	if ctx == nil {
		return nil, fmt.Errorf("connector context is required")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	defaultName = strings.TrimSpace(defaultName)
	set := &Set{SQL: &dsql.SQLComponent{}}
	defer func() {
		if err != nil {
			_ = set.Close()
		}
	}()
	seen := map[string]bool{}
	for _, config := range configs {
		config.Name = strings.TrimSpace(config.Name)
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if config.Name == "" || seen[config.Name] || config.Driver == "" || config.DSN == "" {
			return nil, fmt.Errorf("connectors require unique names, drivers and DSNs")
		}
		seen[config.Name] = true
		const maxMs = int64((1<<63 - 1) / int64(time.Millisecond))
		for _, value := range []int{config.ConnMaxIdleTimeMs, config.ConnMaxLifetimeMs} {
			if int64(value) > maxMs || int64(value) < -maxMs {
				return nil, fmt.Errorf("connector timeout overflows duration")
			}
		}
		dsn := config.DSN
		if config.Secret != nil {
			secret, loadErr := scy.New().Load(ctx, config.Secret)
			if loadErr != nil {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, fmt.Errorf("connector secret loading failed")
			}
			dsn = secret.Expand(dsn)
		}
		db, openErr := sql.Open(config.Driver, dsn)
		if openErr != nil {
			return nil, fmt.Errorf("connector opening failed (driver must be linked in the executable)")
		}
		set.handles = append(set.handles, db)
		if config.MaxIdleConns != 0 {
			db.SetMaxIdleConns(config.MaxIdleConns)
		}
		db.SetMaxOpenConns(config.MaxOpenConns)
		db.SetConnMaxIdleTime(time.Duration(config.ConnMaxIdleTimeMs) * time.Millisecond)
		db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetimeMs) * time.Millisecond)
		if pingErr := db.PingContext(ctx); pingErr != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("connector connection failed")
		}
		if err = set.SQL.RegisterConnector(config.Name, db); err != nil {
			return nil, err
		}
		if config.Name == defaultName {
			set.SQL.DB = db
		}
	}
	if defaultName != "" && set.SQL.DB == nil {
		return nil, fmt.Errorf("default connector is not configured")
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return set, nil
}

func (s *Set) ResolveDB(ctx context.Context, name string) (*sql.DB, error) {
	if s == nil || s.SQL == nil || ctx == nil {
		return nil, fmt.Errorf("connector set and context are required")
	}
	if name == "" && s.SQL.DB != nil {
		return s.SQL.DB, ctx.Err()
	}
	return s.SQL.Connector(ctx, name)
}

func (s *Set) Close() error {
	if s == nil {
		return nil
	}
	var result error
	for _, db := range s.handles {
		result = errors.Join(result, db.Close())
	}
	return result
}
