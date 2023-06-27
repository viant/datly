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
		"gen",
		"-o=patch",
		"-g=campaign",
		"-p=/Users/awitas/go/src/github.com/viant/datly/poc2",
		"-s=/Users/awitas/go/src/github.com/viant/datly/poc2/dsql/campaign/init/campaign_patch.sql",
		"-c=ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/ci_ads?parseTime=true",
	}
	//os.Args = []string{
	//	"",
	//	"dsql",
	//	//"-o=patch",
	//	//"-g=campaign",
	//	"-p=/Users/awitas/go/src/github.com/viant/datly/poc2",
	//	"-s=/Users/awitas/go/src/github.com/viant/datly/poc2/dsql2/handler.sql",
	//	"-c=ci_ads|mysql|root:dev@tcp(127.0.0.1:3306)/ci_ads?parseTime=true",
	//	"-r=/Users/awitas/go/src/github.com/viant/datly/e2e/local/autogen",
	//}

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
