package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"github.com/viant/datly/constant"
	"net/url"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

// schemaOptions composes one explicitly named authoring connection. Opening is
// lazy so connector errors are reported by Validator's normal diagnostics.
type schemaOptions struct {
	Const     *constant.Values
	enabled   bool
	connector string
	driver    string
	dsn       string
	db        *sql.DB
}

func (o *schemaOptions) flags(flags *flag.FlagSet) {
	flags.BoolVar(&o.enabled, "schema", false, "inspect database columns using the configured connector (no execution fixtures)")
	flags.StringVar(&o.connector, "connector", "", "default named discovery connector")
	flags.StringVar(&o.driver, "driver", "", "registered database/sql driver (sqlite3 included)")
	flags.StringVar(&o.dsn, "dsn", "", "discovery connection string; SQLite is opened read-only")
}

func (o *schemaOptions) validate() error {
	if !o.enabled {
		if o.connector != "" || o.driver != "" || o.dsn != "" {
			return fmt.Errorf("connection flags require explicit -schema")
		}
		return nil
	}
	if strings.TrimSpace(o.connector) == "" || strings.TrimSpace(o.driver) == "" || strings.TrimSpace(o.dsn) == "" {
		return fmt.Errorf("-schema requires -connector, -driver and -dsn")
	}
	return nil
}

func (o *schemaOptions) ResolveDB(ctx context.Context, name string) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(name) != strings.TrimSpace(o.connector) {
		return nil, fmt.Errorf("schema connector %q is not configured", name)
	}
	if o.db == nil {
		dsn, err := o.Const.Path(o.dsn)
		if err != nil {
			return nil, fmt.Errorf("discovery DSN: %w", err)
		}
		if o.driver == "sqlite3" {
			var err error
			access := *o
			access.dsn = dsn
			dsn, err = access.sqliteDSN()
			if err != nil {
				return nil, err
			}
		}
		db, err := sql.Open(o.driver, dsn)
		if err != nil {
			return nil, fmt.Errorf("open schema connector %q: %w", name, err)
		}
		o.db = db
	}
	return o.db, nil
}

func (o *schemaOptions) sqliteDSN() (string, error) {
	var location *url.URL
	var err error
	if strings.HasPrefix(o.dsn, "file:") {
		location, err = url.Parse(o.dsn)
		if err != nil || location.Fragment != "" {
			return "", fmt.Errorf("invalid SQLite discovery file URI")
		}
	} else {
		// Bare SQLite DSNs use literal filenames plus optional query options.
		// URL parsing would reinterpret '#' and percent escapes in the filename.
		filename, options, _ := strings.Cut(o.dsn, "?")
		if filename == "" || filename == ":memory:" {
			return "", fmt.Errorf("SQLite discovery requires an existing file database")
		}
		absolute, err := filepath.Abs(filename)
		if err != nil {
			return "", err
		}
		location = &url.URL{Scheme: "file", Path: absolute, RawQuery: options}
	}
	filename := location.Path
	if filename == "" {
		filename, err = url.PathUnescape(location.Opaque)
		if err != nil {
			return "", fmt.Errorf("invalid SQLite discovery filename")
		}
	}
	if filename == "" || filename == ":memory:" {
		return "", fmt.Errorf("SQLite discovery requires an existing file database")
	}
	query, err := url.ParseQuery(location.RawQuery)
	if err != nil {
		return "", fmt.Errorf("invalid SQLite discovery options")
	}
	if query.Get("mode") == "memory" {
		return "", fmt.Errorf("SQLite discovery requires an existing file database")
	}
	query.Set("mode", "ro")
	query.Set("_query_only", "true")
	location.RawQuery = query.Encode()
	return location.String(), nil
}

func (o *schemaOptions) close() {
	if o.db != nil {
		_ = o.db.Close()
	}
}
