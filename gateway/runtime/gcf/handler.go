package gcf

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/afs/gs"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/gateway/app"
	"net/http"
	"os"
)

//Handle handles datly request
func Handle(w http.ResponseWriter, r *http.Request) {
	err := handleRequest(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request) error {
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return fmt.Errorf("config was emrty")
	}
	service, err := app.Singleton(configURL)
	if err != nil {
		return err
	}
	service.Handle(w, r)
	return nil
}
