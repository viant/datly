package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/viant/afs/embed"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/internal/debugruntime"
	"github.com/viant/datly/service/executor/expand"
	_ "github.com/viant/scy/kms/blowfish"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"os"
	"strconv"
	"time"
)

var (
	Version      = "development"
	BuildTimeInS string
)

func init() {
	os.Setenv("DATLY_NOPANIC", "true")
	expand.SetPanicOnError(false)

	if BuildTimeInS != "" {
		seconds, err := strconv.Atoi(BuildTimeInS)
		if err != nil {
			panic(err)
		}
		env.BuildTime = time.Unix(int64(seconds), 0)
	}
}

func main() {
	debugruntime.StartGopsFromEnv()
	if err := cmd.RunApp(Version, os.Args[1:]); err != nil {
		panic(err)
	}
}
