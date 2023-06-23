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

	os.Args = []string{
		"",
		"dsql",
		"-p=/Users/klarysz/Documents/github_viant/datly/poc",
		"-s=dsql/handler.sql",
		"-c=ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/ci_ads?parseTime=true",
		"-r=autogen",
	}

	server, err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		log.Fatal(err)
	}

	if server != nil {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err.Error())
		}
	}
}
