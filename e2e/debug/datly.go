package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gops/agent"
	_ "github.com/viant/afs/embed"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/service/executor/expand"
	_ "github.com/viant/dyndb"
	_ "github.com/viant/scy/kms/blowfish"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/update"
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"log"
	"os"
	"path"
	"strconv"
	"time"
)

var (
	Version      = "development"
	BuildTimeInS string
)

func init() {

	os.Setenv("DATLY_NOPANIC", "true")
	read.ShowSQL(true)
	update.ShowSQL(true)
	insert.ShowSQL(true)
	expand.SetPanicOnError(false)

	if BuildTimeInS != "" {
		seconds, err := strconv.Atoi(BuildTimeInS)
		if err != nil {
			panic(err)
		}

		env.BuildTime = time.Unix(int64(seconds), 0)
	}
}

type ConsoleWriter struct {
}

func (c *ConsoleWriter) Write(data []byte) (n int, err error) {
	fmt.Println(string(data))
	return len(data), nil
}

func main() {
	os.Setenv("DATLY_NOPANIC", "0")

	//baseDir := toolbox.CallerDirectory(3)
	//configURL := filepath.Join(baseDir, "../local/autogen/Datly/config.json")

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret/viant-e2e.json"))
	os.Chdir("/Users/michal/Go/src/github.vianttech.com/adelphic/datly-forecasting")

	//configURL := "/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/repo/dev/Datly/config.json"
	//
	//os.Args = []string{
	//	"",
	//	"-c=" + configURL,
	//	"-z=/tmp/jobs/datly",
	//}

	//os.Args = []string{
	//	"",
	//	"initExt",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//}

	//
	//
	//datly
	//dsql
	//-s=dsql/actor/Actor_patch.sql \
	//-p=~/myproject3 \
	//-c='sakiladb|mysql|root:p_ssW0rd@tcp(127.0.0.1:3306)/sakila?parseTime=true' \
	//-r=repo/dev

	//os.Args = []string{
	//	"",
	//	"dsql",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//}

	//os.Args = []string{
	//	"",
	//	"dsql",
	//	"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/forecasting/total.sql",
	//	"-u=forecasting",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//	"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
	//	"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime = true",
	//	"-r=repo/dev",
	//}

	//os.Args = []string{
	//	"",
	//	"dsql",
	//	"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/forecasting/total.sql",
	//	"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/forecasting/multixml.sql",
	//	"-u=forecasting",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//	"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
	//	"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true",
	//	"-S=dsql/forecasting/shared/substitutes.yaml",
	//	"-r=repo/dev",
	//}

	//os.Args = []string{
	//	"",
	//	"dsql",
	//	"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/forecasting/empty.sql",
	//	"-u=forecasting",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//	"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
	//	"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true",
	//	"-S=dsql/forecasting/shared/substitutes.yaml",
	//	"-r=repo/dev",
	//}

	//os.Args = []string{
	//	"",
	//	"dsql",
	//	"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/forecasting/empty.sql",
	//	"-u=forecasting",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//	"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
	//	"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true",
	//	"-S=dsql/forecasting/shared/substitutes.yaml",
	//	"-r=repo/dev",
	//}

	//os.Args = []string{
	//	"",
	//	"translate",
	//	"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/multitab.sql",
	//	"-u=forecasting",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//	"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
	//	"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true",
	//	"-S=dql/forecasting/shared/keys_project.yaml",
	//	"-S=dql/forecasting/shared/keys_app.yaml",
	//	"-r=repo/dev",
	//}

	os.Args = []string{
		"",
		"translate",
		"-s=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/multitab.sql",
		"-u=forecasting",
		"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
		"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
		"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true",
		"-S=dql/forecasting/shared/keys_project.yaml",
		"-S=dql/forecasting/shared/keys_app.yaml",
		"-r=repo/dev",
	}

	//os.Args = []string{
	//	"",
	//	"dsql",
	//	"-p=/Users/michael/Go/src/github.vianttech.com/adelphic/datly-forecasting",
	//}

	fmt.Printf("[INFO] Build time: %v\n", env.BuildTime.String())

	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()
	err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		log.Fatal(err)
	}
}
