//go:build debug

package env

import (
	delete2 "github.com/viant/sqlx/io/delete"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	"os"
)

func init() {
	os.Setenv("DATLY_DEBUG", "true")
	insert.ShowSQL(true)
	update.ShowSQL(true)
	read.ShowSQL(true)
	delete2.ShowSQL(true)
}
