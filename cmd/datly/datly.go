package main

import (
	"fmt"
	"github.com/google/gops/agent"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
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

	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HOME"), ".secret/viant-e2e.json"))
	//
	os.Chdir("/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting")
	os.Args = []string{
		"datly",
		"run",
		"-=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/view/media_type.sql",
		"-p=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting",
		"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
		"-r=repo/dev",
	}
	//datly -N=run_tests_one_to_many -X=/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/cases/001_one_to_many/vendor_list.sql -w=autogen -C='dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true' -C='dyndb|dynamodb|dynamodb://localhost:8000/us-west-1?key=dummy&secret=dummy'  -j='/Users/awitas/go/src/github.com/viant/datly/e2e/local/jwt/public.enc|blowfish://default' -m='/Users/awitas/go/src/github.com/viant/datly/e2e/local/jwt/hmac.enc|blowfish://default' --partialConfig='/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/partial_config.json'

	////

	err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

}
