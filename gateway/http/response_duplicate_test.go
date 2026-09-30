package http

import (
	"errors"
	stdhttp "net/http"
	"testing"

	"github.com/viant/sqlx/io/errx"
	xresponse "github.com/viant/xdatly/response"
)

func TestDuplicateKeyIsConflictWithoutOverridingExplicitStatus(t *testing.T) {
	private := errx.DuplicateKey("insert", "namespaces", errors.New("private database detail"))
	if got := classifyRequestError(private); got != stdhttp.StatusConflict {
		t.Fatalf("duplicate key status=%d, want 409", got)
	}
	if got := classifyRequestError(&xresponse.Error{Code: stdhttp.StatusBadRequest, Cause: private}); got != stdhttp.StatusBadRequest {
		t.Fatalf("explicit status=%d, want 400", got)
	}
	if got := classifyRequestError(errors.New("unclassified internal error")); got != stdhttp.StatusInternalServerError {
		t.Fatalf("unknown error status=%d, want 500", got)
	}
}
