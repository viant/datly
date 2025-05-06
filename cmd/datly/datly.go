package main

import (
	"fmt"
	"github.com/google/gops/agent"
	"github.com/viant/datly"
	"github.com/viant/datly/cmd"
	"github.com/viant/datly/cmd/env"
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
	go func() {
		if err := agent.Listen(agent.Options{}); err != nil {
			log.Fatal(err)
		}
	}()

	//os.Args = []string{"",
	//	"mcp",
	//	"-c=config.json",
	//	"-C=oauth_local.json",
	//	"-A=F",
	//}

	err := cmd.RunApp(datly.Version, os.Args[1:])
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		log.Fatal(err)
	}

	/*
		{"id":1,"jsonrpc":"2.0","method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"tester","version":"0.1"},"protocolVersion":"2025-03-26"}}

		{"id":null,"jsonrpc":"2.0","method":"notification/initialized"}

		{"id":2,"jsonrpc":"2.0","method":"tools/list","params":{}}


		{"id":3,"jsonrpc":"2.0","method":"tools/call","params":{"arguments":{},"name":"vendor"}}
		//vendor

	*/

}
