//go:build debug

package env

import (
	"github.com/viant/datly/utils/debug"
	delete2 "github.com/viant/sqlx/io/delete"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
)

func init() {
	debug.SetEnabled(true)
	insert.ShowSQL(true)
	update.ShowSQL(true)
	read.ShowSQL(true)
	delete2.ShowSQL(true)
}
