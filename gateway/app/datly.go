package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/gateway/runtime/standalone"
	"os"
)

//Version app version passed with ldflags i.e -ldflags="-X 'main.Version=v1.0.0'"
var Version = "development"

func main() {
	standalone.RunApp(Version, os.Args[1:])
}
