package main

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	"github.com/viant/datly/app/aws/lambda"
)

func main() {
	lambda.StartReader()
}
