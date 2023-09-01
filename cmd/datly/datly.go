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

	//os.Args = []string{"",
	//	"-N=run_tests_one_to_many",
	//	"-X=/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/cases/022_generate_post_one_one/post_one_one.sql",
	//	"-w=autogen",
	//	"-C=dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
	//}

	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()

	err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

}
