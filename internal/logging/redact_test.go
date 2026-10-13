package logging

import (
	"bytes"
	"strings"
	"testing"

	"github.com/viant/xdatly/response"
)

func TestFailureReasonRedactionAcrossCompatibilityPaths(t *testing.T) {
	for _, tc := range []struct {
		message, want string
		private       []string
	}{
		{"outer: failed to run query: select 'sql-secret', due to Error 1146 (42S02): Table 'metadata.audience' doesn't exist", "Table 'metadata.audience' doesn't exist", []string{"sql-secret"}},
		{"failed to scan select 'scan-secret', due to conversion failed", "conversion failed", []string{"scan-secret"}},
		{"nested, due to read: context canceled", "nested, due to read: context canceled", nil},
		{"nested: context deadline exceeded", "nested: context deadline exceeded", nil},
		{"Error 1142 (42000): SELECT command denied for table 'adOrder'", "SELECT command denied for table 'adOrder'", nil},
		{"connect: connection refused mysql://user:url-secret@host/db", "connection refused", []string{"url-secret", "user:"}},
		{"connect: connection refused user:dsn-secret@tcp(host:3306)/db", "connection refused", []string{"dsn-secret", "user:"}},
		{"connect: connection refused user:unix-secret@unix(/tmp/mysql.sock)/db", "connection refused", []string{"unix-secret"}},
		{`auth failed Authorization: Bearer bearer-secret password='password-secret' password="space secret" "client_secret":"client-secret" token=token-secret&x=1`, "auth failed", []string{"bearer-secret", "password-secret", "space secret", "client-secret", "token-secret"}},
		{"auth failed Basic YmFzaWM6c2VjcmV0 eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwcml2YXRlIn0.abcdefghijk", "auth failed", []string{"YmFzaWM6c2VjcmV0", "eyJhbGci"}},
		{"auth failed Authorization: Bearer\r\nfolded-secret", "auth failed", []string{"folded-secret"}},
		{"connect: connection refused user:default-dsn-secret@host/db", "connection refused", []string{"default-dsn-secret"}},
		{"upstream failed\r\nSet-Cookie: session=cookie-secret; tracking=tracking-secret\r\nheaders={X-Private: header-secret}", "upstream failed", []string{"cookie-secret", "tracking-secret", "header-secret"}},
		{"upstream failed body={\"private\":\"body-secret\"}", "upstream failed", []string{"body-secret"}},
		{"query failed args=[positional-secret]", "query failed", []string{"positional-secret"}},
	} {
		t.Run(tc.want, func(t *testing.T) {
			for _, includeSQL := range []bool{false, true} {
				var output bytes.Buffer
				s := NewSink(&output, false, false, includeSQL)
				s.sqlRead("trace\r\nforged", "view\nforged", &response.SQLExecution{Error: tc.message, SQL: "select ?", Args: []any{"bind-secret"}}, nil)
				s.Read("trace", &response.Metric{View: "view", Error: tc.message})
				got := output.String()
				if !strings.Contains(got, tc.want) {
					t.Fatalf("reason lost: %s", got)
				}
				for _, private := range append(tc.private, "bind-secret") {
					if strings.Contains(got, private) {
						t.Fatalf("leaked %s: %s", private, got)
					}
				}
				if strings.Count(got, "\n") != 2 || strings.Contains(got, "\r") {
					t.Fatalf("record injection: %q", got)
				}
			}
		})
	}
}

func TestKnownQueryContainingCauseDelimiter(t *testing.T) {
	query := "select 'not a cause, due to sql-secret'"
	got := FailureReason("failed to run query: "+query+", due to context canceled", query)
	if strings.Contains(got, "sql-secret") || !strings.HasSuffix(got, ", due to context canceled") {
		t.Fatalf("query content confused cause redaction: %s", got)
	}
}

func TestOptInSQLRedactsCredentials(t *testing.T) {
	var output bytes.Buffer
	NewSink(&output, false, false, true).sqlRead("trace", "view", &response.SQLExecution{SQL: "select * from credentials where password='sql-password-secret' and token='sql-token-secret'\r\n", Error: "connection refused"}, nil)
	got := output.String()
	if !strings.Contains(got, `sql="select * from credentials`) || strings.Contains(got, "sql-password-secret") || strings.Contains(got, "sql-token-secret") || strings.Count(got, "\n") != 1 {
		t.Fatalf("unsafe SQL: %s", got)
	}
}
