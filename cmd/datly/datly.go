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

	//os.Chdir("/Users/awitas/go/src/github.com/viant/datly/e2e/local")
	//os.Args = []string{"",
	//	"-N=run_tests_read_async",
	//	"-X=/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/cases/043_json_codec_single/json_codec_single.sql",
	//	//"-X=/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/cases/061_read_async/vendor_auth_async.sql",
	//	"-w=autogen",
	//	"-C=dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
	//}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path.Join(os.Getenv("HONE"), "./secret/viant-e2e.json"))
	os.Chdir("/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting")

	//os.Args = []string{"",
	//	"dsql",
	//	//"-s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/view/total.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/view/ad_order_total.sql",
	//	"-s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dsql/view/multitab_v1.sql",
	//	"-p=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting",
	//	"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
	//	"-c=dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
	//	"-r=repo/dev",
	//}

	err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

}
