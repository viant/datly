package column

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"google.golang.org/api/googleapi"
)

func TestDiscoveryErrorPreservesNativeSQLiteCauseWithoutSourceProjection(t *testing.T) {
	h := sqlite.New(t)
	view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: "SELECT ID FROM UNAVAILABLE_RECORDS"}}
	err := New(Connections{"source": h.DB}).Refine(context.Background(), &spec.Component{
		Name: "reader", Settings: &spec.Settings{DefaultConnector: "source"}, RootView: view,
	}, nil, nil)
	var discovery *DiscoveryError
	if !errors.As(err, &discovery) || discovery.Unwrap() == nil {
		t.Fatalf("native discovery error type/cause lost: %T", err)
	}
	d := discovery.Diagnostic()
	if d.Operation != "query" || d.View != "Records" {
		t.Fatalf("native diagnostic=%+v", d)
	}
	// SQLite exposes its own native error, but no guess is made by reading its
	// English message. Drivers without a registered structured mapping omit it.
	if d.DriverCode != "" || d.Reason != "" {
		t.Fatalf("invented reason: %+v", d)
	}
	if !strings.Contains(discovery.Error(), "SQLX discovery failed for") || !strings.Contains(discovery.Error(), "UNAVAILABLE_RECORDS") {
		t.Fatal("legacy trusted error format changed")
	}
	assertSafeDiscoveryJSON(t, d)
}

func TestDiscoveryErrorSafeDTOAndLegacyErrorChain(t *testing.T) {
	query := "SELECT 'CANARY_DATA' FROM records /* https://user:CANARY_PASSWORD@source.invalid/?token=CANARY_TOKEN */"
	for _, view := range []string{"Records_2", "", "2Records", "records.name", "https://CANARY_TOKEN.invalid", "CANARY_DATA\nRecords", strings.Repeat("x", 129)} {
		t.Run(fmt.Sprintf("view_%d_%t", len(view), validDiscoveryIdentifier(view)), func(t *testing.T) {
			cause := errors.New("CANARY_PASSWORD https://source.invalid SELECT CANARY_DATA")
			err := newDiscoveryError(view, query, cause)
			if !errors.Is(err, cause) || err.Error() != fmt.Sprintf("SQLX discovery failed for %q: %v", query, cause) {
				t.Fatal("legacy error chain/format changed")
			}
			d := err.Diagnostic()
			if d.Operation != "query" || d.DriverCode != "" || d.Reason != "" {
				t.Fatalf("invented native detail: %+v", d)
			}
			if view == "Records_2" && d.View != view || view != "Records_2" && d.View != "" {
				t.Fatalf("unsafe view: %+v", d)
			}
			d.View = "caller mutation"
			if err.Diagnostic().View == d.View {
				t.Fatal("diagnostic aliases caller state")
			}
			assertSafeDiscoveryJSON(t, err.Diagnostic())
			assertSafeDiscoveryJSON(t, err)
		})
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if err := newDiscoveryError("Records", query, fmt.Errorf("native query: %w", cause)); !errors.Is(err, cause) {
			t.Fatal("cancellation lost")
		}
	}
}

func TestDiscoveryErrorMapsOnlyStructuredKnownDriverValues(t *testing.T) {
	for _, tc := range []struct {
		name         string
		cause        error
		code, reason string
	}{
		{"mysql missing source", &mysql.MySQLError{Number: 1146, Message: "CANARY_DATA"}, "mysql:1146", "source_not_found"},
		{"mysql unknown", &mysql.MySQLError{Number: 65000, Message: "invalidQuery CANARY_TOKEN"}, "", ""},
		{"api query", &googleapi.Error{Code: 400, Message: "CANARY_PASSWORD", Body: "SELECT CANARY_DATA", Errors: []googleapi.ErrorItem{{Reason: "invalidQuery", Message: "CANARY_TOKEN"}}}, "googleapi:400", "query_invalid"},
		{"api unknown reason", &googleapi.Error{Code: 403, Errors: []googleapi.ErrorItem{{Reason: "CANARY_TOKEN"}}}, "googleapi:403", ""},
		{"api unknown code", &googleapi.Error{Code: 999, Errors: []googleapi.ErrorItem{{Reason: "notFound"}}}, "", "source_not_found"},
		{"text is not authority", errors.New("googleapi:400 invalidQuery mysql:1146 CANARY_TOKEN"), "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := newDiscoveryError("Records", "SELECT CANARY_DATA", fmt.Errorf("CANARY_PASSWORD: %w", tc.cause))
			d := err.Diagnostic()
			if d.DriverCode != tc.code || d.Reason != tc.reason || !errors.Is(err, tc.cause) {
				t.Fatalf("diagnostic=%+v", d)
			}
			assertSafeDiscoveryJSON(t, d)
		})
	}
}

func assertSafeDiscoveryJSON(t *testing.T, value any) {
	t.Helper()
	wire, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	for _, token := range []string{"CANARY_", "https://", "SELECT", "source.invalid", "UNAVAILABLE_RECORDS"} {
		if strings.Contains(string(wire), token) {
			t.Fatalf("native source leaked in DTO: %s", wire)
		}
	}
}
