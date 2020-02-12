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
	"os"
)

func main() {

	//CACHE_URL is to serve content over 6MB and for caching vi s3, see lambda 6MB limits
	cacheURL := os.Getenv("CACHE_URL")
	if cacheURL == "" {
		cacheURL = "mem://localhost/cache"
	}
	lambda.StartReader(cacheURL)
}
