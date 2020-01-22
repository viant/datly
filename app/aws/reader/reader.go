package main

import (
	"datly/app/aws/lambda"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/afsc/gs"

)


func main() {
	lambda.StartReader()
}

