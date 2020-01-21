package main

import (
	"context"
	"datly/reader"
	"datly/singleton"
	"flag"
	"fmt"
	_ "github.com/MichaelS11/go-cql-driver"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	"github.com/viant/toolbox/url"
	"log"
	"net/http"
	"os"
)

var configURL = flag.String("configURL", "config.json", "config URL")

func main() {
	flag.Parse()
	ctx := context.Background()
	URL := url.NewResource(*configURL).URL
	fmt.Printf("using config: %v\n", URL)
	service, err := singleton.Reader(ctx, URL)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", reader.HandleRead(service))
	http.HandleFunc("/status/", func(writer http.ResponseWriter, request *http.Request) {
		if request.ContentLength > 0 {
			_ = request.Body.Close()
		}
		_, _ = writer.Write([]byte("up"))
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("listening on %v\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
