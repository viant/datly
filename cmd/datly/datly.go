package main

import (
	"fmt"
	"github.com/google/gops/agent"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
	"log"
	"os"
	"strconv"
	"time"
)

var (
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

type ConsoleWriter struct{}

func (c *ConsoleWriter) Write(data []byte) (n int, err error) {
	fmt.Println(string(data))
	return len(data), nil
}

func main() {
	fmt.Printf("[INFO] Build time: %v\n", env.BuildTime.String())

	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()
	//
	os.Chdir("/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting")
	os.Args = []string{
		"",
		"dsql",
		"-u=forecasting",
		"-s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/forecasting/total.sql",
		"-p=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting",
		"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
		"-c=ci_datly|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
	}

	err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

}
