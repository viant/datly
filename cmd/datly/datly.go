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

	os.Chdir("/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting")
	os.Args = []string{"",
		"translate",
		"-s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/multitab.sql",
		"-u=forecasting",
		"-p=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting",
		"-c=ci_event|bigquery|bigquery://viant-e2e/ci_event",
		"-c=datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true",
		"-S=dql/forecasting/shared/keys_project.yaml",
		"-S=dql/forecasting/shared/keys_app.yaml",
		"-r=repo/dev"}
	err := cmd.New(Version, os.Args[1:], &ConsoleWriter{})
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

}

/*
/usr/local/bin/datly translate -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/static.dql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/total.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/ad_order_total.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/country.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/country_region.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/device_type.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/hhi.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/hourly.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_oses.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/age.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/iab.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/language.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/creative_size.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_dma.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/creative_type.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/site_type.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/neustar_segments.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/media_type.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/gender.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_brands.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_carriers.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_models.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_publishers.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_city.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/daily.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_sites.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/top_sites_long.sql -s=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting/dql/forecasting/adelaide_grouping.sql -u=forecasting -p=/Users/awitas/go/src/github.vianttech.com/adelphic/datly-forecasting -c='ci_event|bigquery|bigquery://viant-e2e/ci_event' -c='datly_jobs|mysql|root:dev@tcp(127.0.0.1:3306)/datly_jobs?parseTime=true' -S='dql/forecasting/shared/keys_project.yaml' -S='dql/forecasting/shared/keys_app.yaml' -r=repo/dev
[run[test]run|[translateDSQL]exec.run awitas@localhost:22                                                                                                                                               stdout]
[I
*/
