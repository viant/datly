package main

import (
	"fmt"
	"github.com/viant/datly"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/internal/debugruntime"
	"log"
	"os"
	"strconv"
	"time"
)

var Version string
var (
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
	debugruntime.StartGopsFromEnv()
	err := cmd.RunApp(datly.Version, os.Args[1:])
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

}
