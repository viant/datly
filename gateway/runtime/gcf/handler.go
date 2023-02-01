package gcf

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/xdatly"
	_ "github.com/viant/scy/kms/blowfish"
	"net/http"
	"os"
	"time"
)

//Handle handles datly request
func Handle(w http.ResponseWriter, r *http.Request) {
	err := handleRequest(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

//GCF doesn't include the function name in the URL segments
func handleRequest(w http.ResponseWriter, r *http.Request) error {
	now := time.Now()
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return fmt.Errorf("config was emrty")
	}

	service, err := gateway.Singleton(configURL, nil, nil, xdatly.Config, nil)
	if err != nil {
		return err
	}
	service.LogInitTimeIfNeeded(now, w)

	service.ServeHTTP(w, r)
	return nil
}
