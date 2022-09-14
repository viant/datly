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
	"github.com/viant/datly/gateway/registry"
	_ "github.com/viant/scy/kms/blowfish"
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

//GCF doesn't include the function name in the URL segments
func handleRequest(w http.ResponseWriter, r *http.Request) error {
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return fmt.Errorf("config was emrty")
	}

	service, err := gateway.Singleton(configURL, nil, nil, registry.Codecs, registry.Types, nil)
	if err != nil {
		return err
	}

	service.ServeHTTP(w, r)
	return nil
}
