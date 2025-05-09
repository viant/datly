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
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/toolbox"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

var (
	Version      = "development"
	BuildTimeInS string
)

func init() {

	os.Setenv("DATLY_NOPANIC", "true")
	//read.ShowSQL(true)
	//update.ShowSQL(true)
	//insert.ShowSQL(true)
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
	//os.Setenv("DATLY_NOPANIC", "0")

	baseDir := toolbox.CallerDirectory(3)

	os.Chdir(filepath.Join(baseDir, "../local/autogen/Datly"))
	configURL := filepath.Join(baseDir, "../local/autogen/Datly/config.json")
	os.Args = []string{
		"",
		"mcp",
		//"-p=4981",
		"-c=" + configURL,
		"-z=/tmp/jobs/datly",
	}

	//fmt.Printf("[INFO] Build time: %v\n", env.BuildTime.String())
	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()
	err := cmd.RunApp(Version, os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
}
