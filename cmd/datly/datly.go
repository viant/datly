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
	//os.Args = []string{
	//	"",
	//	"-N=run_tests_generate_post_basic_one",
	//	"-X=/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/cases/022_generate_post_one_one/post_one_one.sql",
	//	"-w=autogen",
	//	"-C=dev|mysql|root:dev@tcp(127.0.0.1:3306)/dev?parseTime=true",
	//	"-C=dyndb|dynamodb|dynamodb://localhost:8000/us-west-1?key=dummy&secret=dummy",
	//	"-j=/Users/awitas/go/src/github.com/viant/datly/e2e/local/jwt/public.enc|blowfish://default",
	//	"-m=/Users/awitas/go/src/github.com/viant/datly/e2e/local/jwt/hmac.enc|blowfish://default",
	//	"--partialConfig=/Users/awitas/go/src/github.com/viant/datly/e2e/local/regression/partial_config.json",
	//	"-G=post",
	//}
	//os.Args = []string{
	//"",
	//"gen",
	//"-o=patch",
	//"-g=campaign",
	//"-p=/Users/awitas/go/src/github.com/viant/datly/poc",
	//"-s=/Users/awitas/go/src/github.com/viant/datly/poc/dsql/campaign/init/campaign_patch.sql",
	//"-c=ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/ci_ads?parseTime=true",

	/*
		"",
		"dsql",
		"-p=/Users/awitas/go/src/github.com/viant/datly/poc",
		"-s=/Users/awitas/go/src/github.com/viant/datly/poc/dsql/campaign/campaign_patch.sql",
		"-c=ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/ci_ads?parseTime=true",
	*/

	//	}
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
