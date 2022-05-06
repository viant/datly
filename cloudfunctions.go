package datly

import (
	"github.com/viant/datly/gateway/runtime/gcf"
	"net/http"
)

func Handle(w http.ResponseWriter, r *http.Request) {
	gcf.Handle(w, r)
}
