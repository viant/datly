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
	_ "github.com/viant/asc"
	_ "github.com/viant/bgc"
	"log"
	"net/http"
	"os"

	_ "github.com/adrianwit/dyndb"
	_ "github.com/adrianwit/fbc"
	_ "github.com/adrianwit/fsc"
	_ "github.com/adrianwit/mgc"
)
var configURL = flag.String("configURL", "config.json", "config URL")

func main()  {
	flag.Parse()
	ctx := context.Background()

	fmt.Printf("config: %v\n", *configURL)
	service, err  := singleton.Reader(ctx, *configURL)
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", reader.HandleRead(service))
	http.HandleFunc("/status/", func(writer http.ResponseWriter, request *http.Request) {
		if request.ContentLength > 0 {
			_ = request.Body.Close()
		}
		_ , _ = writer.Write([]byte("up"))
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("listening on %v\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}

