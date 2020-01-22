package server

import (
	"context"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/singleton"
	"flag"
	"fmt"
	"github.com/viant/toolbox/url"
	"log"
	"net/http"
	"os"
)

var configURL = flag.String("configURL", "config.json", "config URL")

func StartServer() {

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
