package gcf

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/afs/embed"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	_ "github.com/viant/cloudless/async/mbus/aws"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/view/extension"
	_ "github.com/viant/dyndb"
	_ "github.com/viant/scy/kms/blowfish"
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"net/http"
	"os"
)

// Handle handles datly request
func Handle(w http.ResponseWriter, r *http.Request) {
	err := handleRequest(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// GCF doesn't include the function name in the URL segments
func handleRequest(w http.ResponseWriter, r *http.Request) error {
	configURL := os.Getenv("CONFIG_URL")
	if configURL == "" {
		return fmt.Errorf("config was emrty")
	}
	ctx := context.Background()
	service, err := gateway.Singleton(ctx, gateway.WithConfigURL(configURL), gateway.WithExtensions(extension.Config))
	if err != nil {
		return err
	}
	service.ServeHTTP(w, r)
	return nil
}
