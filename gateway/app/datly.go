package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/gops/agent"
	_ "github.com/lib/pq"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/gateway/runtime/standalone"
	"log"
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
	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Printf("[WARN] failed to start gops agent: %v", err)
		}
	}()
	standalone.RunApp(Version, os.Args[1:])
}
