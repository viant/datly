package column

import (
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"google.golang.org/api/googleapi"
)

// DiscoveryDiagnostic is the safe, source-free projection of a native discovery
// failure. DriverCode and Reason are omitted unless a structured native driver
// error supplies an explicitly recognized value. They never come from Error().
type DiscoveryDiagnostic struct {
	Operation  string `json:"operation"`
	View       string `json:"view,omitempty"`
	DriverCode string `json:"driverCode,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// DiscoveryError preserves native query/cause evidence for trusted Go callers.
// Use Diagnostic for serialization; the query and native cause are private.
type DiscoveryError struct {
	query      string
	cause      error
	diagnostic DiscoveryDiagnostic
}

func (e *DiscoveryError) Error() string {
	return fmt.Sprintf("SQLX discovery failed for %q: %v", e.query, e.cause)
}

func (e *DiscoveryError) Unwrap() error { return e.cause }

func (e *DiscoveryError) Diagnostic() DiscoveryDiagnostic { return e.diagnostic }

func newDiscoveryError(view, query string, cause error) *DiscoveryError {
	d := DiscoveryDiagnostic{Operation: "query"}
	// Only an authored identifier can become view metadata. Connector names,
	// paths, expressions, quoted strings and arbitrary driver text cannot.
	if validDiscoveryIdentifier(view) {
		d.View = view
	}
	d.DriverCode, d.Reason = discoveryDriverReason(cause)
	return &DiscoveryError{query: query, cause: cause, diagnostic: d}
}

func validDiscoveryIdentifier(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for i, c := range []byte(value) {
		if c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

func discoveryDriverReason(err error) (string, string) {
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		switch my.Number {
		case 1045:
			return "mysql:1045", "access_denied"
		case 1054:
			return "mysql:1054", "column_not_found"
		case 1064:
			return "mysql:1064", "query_invalid"
		case 1146:
			return "mysql:1146", "source_not_found"
		}
	}
	var api *googleapi.Error
	if errors.As(err, &api) {
		code := ""
		switch api.Code {
		case 400, 401, 403, 404, 408, 409, 429, 500, 502, 503, 504:
			code = fmt.Sprintf("googleapi:%d", api.Code)
		}
		for _, item := range api.Errors {
			switch item.Reason {
			case "invalidQuery":
				return code, "query_invalid"
			case "notFound":
				return code, "source_not_found"
			case "accessDenied":
				return code, "access_denied"
			case "backendError", "internalError":
				return code, "source_unavailable"
			case "rateLimitExceeded", "jobRateLimitExceeded":
				return code, "rate_limited"
			case "quotaExceeded":
				return code, "quota_exceeded"
			}
		}
		return code, ""
	}
	return "", ""
}
