package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/gateway/runtime/standalone"
	"os"
	"strconv"
	"time"
)

var (
	//Version app version passed with ldflags i.e -ldflags="-X 'main.Version=v1.0.0'"
	Version      = "development"
	BuildTimeInS string
)

func init() {
	if BuildTimeInS != "" {
		seconds, err := strconv.Atoi(BuildTimeInS)
		if err != nil {
			panic(err)
		}

		env.BuildTime = time.Unix(int64(seconds), 0)
	}
}

func main() {
	standalone.RunApp(Version, os.Args[1:])
}
