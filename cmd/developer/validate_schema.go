package developer

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/viant/datly/constant"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

// schemaOptions composes explicitly named authoring connections. Opening is
// lazy so connector errors are reported by Validator's normal diagnostics.
type schemaOptions struct {
	Const          *constant.Values
	enabled        bool
	connector      string
	driver         string
	dsn            string
	db             *sql.DB
	connectorsFile string
	connections    map[string]*schemaOptions
	mu             sync.Mutex
}

func (o *schemaOptions) flags(flags *flag.FlagSet) {
	flags.BoolVar(&o.enabled, "schema", false, "inspect database columns using the configured connector (no execution fixtures)")
	flags.StringVar(&o.connector, "connector", "", "default named discovery connector")
	flags.StringVar(&o.driver, "driver", "", "registered database/sql driver (sqlite3 included)")
	flags.StringVar(&o.dsn, "dsn", "", "discovery connection string; SQLite is opened read-only")
	flags.StringVar(&o.connectorsFile, "schema-connectors", "", "local JSON array of named discovery connectors (name, driver, dsn)")
}

func (o *schemaOptions) validate() error {
	if !o.enabled {
		if o.connector != "" || o.driver != "" || o.dsn != "" || o.connectorsFile != "" {
			return fmt.Errorf("connection flags require explicit -schema")
		}
		return nil
	}
	if strings.TrimSpace(o.connector) == "" {
		return fmt.Errorf("-schema requires -connector, -driver and -dsn, or -schema-connectors")
	}
	if o.connectorsFile == "" {
		if strings.TrimSpace(o.driver) == "" || strings.TrimSpace(o.dsn) == "" {
			return fmt.Errorf("-schema requires -connector, -driver and -dsn")
		}
		return nil
	}
	if o.driver != "" || o.dsn != "" {
		return fmt.Errorf("-schema-connectors cannot be combined with -driver or -dsn")
	}
	file, err := os.Open(o.connectorsFile)
	if err != nil {
		return fmt.Errorf("read schema connectors: %w", err)
	}
	defer file.Close()
	var entries []struct {
		Name   string `json:"name"`
		Driver string `json:"driver"`
		DSN    string `json:"dsn"`
	}
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&entries); err != nil {
		return fmt.Errorf("decode schema connectors: %w", err)
	}
	var trailing any
	if err = decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("schema connectors must contain exactly one JSON array")
	}
	connections := make(map[string]*schemaOptions, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" || strings.TrimSpace(entry.Driver) == "" || strings.TrimSpace(entry.DSN) == "" {
			return fmt.Errorf("schema connectors require name, driver and dsn")
		}
		if connections[name] != nil {
			return fmt.Errorf("duplicate schema connector %q", name)
		}
		connections[name] = &schemaOptions{connector: name, driver: entry.Driver, dsn: entry.DSN}
	}
	if connections[strings.TrimSpace(o.connector)] == nil {
		return fmt.Errorf("default schema connector %q is not configured", o.connector)
	}
	o.connections = connections
	return nil
}

func (o *schemaOptions) ResolveDB(ctx context.Context, name string) (*sql.DB, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o.connections != nil {
		entry := o.connections[strings.TrimSpace(name)]
		if entry == nil {
			return nil, fmt.Errorf("schema connector %q is not configured", name)
		}
		entry.Const = o.Const
		return entry.ResolveDB(ctx, name)
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
			access := schemaOptions{dsn: dsn}
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
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, entry := range o.connections {
		entry.close()
	}
	if o.db != nil {
		_ = o.db.Close()
	}
}
