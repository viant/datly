package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/xdatly/response"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestCaptureWithoutLoggerDoesNotWriteStdout(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = writer
	defer func() { os.Stdout = previous; reader.Close(); writer.Close() }()
	recorder := &Recorder{}
	recorder.Read("", &response.Metric{View: "records", Rows: 1})
	recorder.Read("", &response.Metric{View: "records", Error: "prepare: connection refused"})
	recorder.SQL("", "records", &response.SQLExecution{Error: "private error", CacheStats: &response.CacheStats{FoundLazy: true}})
	writer.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("unconfigured capture wrote stdout: %s", data)
	}
}

func TestNativeFailureReasonsAndContext(t *testing.T) {
	for _, reason := range []string{
		"Error 1142 (42000): SELECT command denied to user 'reader'@'host' for table 'campaign_flight'",
		"Error 1146 (42S02): Table 'metadata.audience' doesn't exist",
		"Error 1045 (28000): Access denied for user 'reader'@'host' (using password: YES)",
		"dial tcp 127.0.0.1:3306: connect: connection refused",
		fmt.Errorf("read audience: %w", context.Canceled).Error(),
		fmt.Errorf("read adOrder: %w", context.DeadlineExceeded).Error(),
	} {
		t.Run(reason, func(t *testing.T) {
			var output bytes.Buffer
			r := NewRecorder(slog.New(slog.NewJSONHandler(&output, nil)))
			start := time.Now()
			e := &response.SQLExecution{ID: "execution", ParentID: "parent", SQL: "select private_literal from campaign_flight where id = ?", Args: []any{"private-bind"}, Rows: 2, StartTime: start, EndTime: start.Add(3 * time.Millisecond), Error: "failed to run query: select private_literal from campaign_flight where id = ?, due to " + reason, CacheStats: &response.CacheStats{Type: "lazy", Namespace: "metadata", Dataset: "flights", ErrorType: "TIMEOUT", ErrorCode: 9, Key: "private-key"}}
			m := &response.Metric{ID: "metric", View: "campaign_flight", Rows: 2, Elapsed: "3ms", Error: e.Error, Executions: response.SQLExecutions{e}}
			before, _ := json.Marshal(m)
			r.SQL("trace", m.View, e)
			r.Read("trace", m)
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			if len(lines) != 3 {
				t.Fatalf("expected cache, SQL and completion: %s", output.String())
			}
			for i, line := range lines {
				var record map[string]any
				if err := json.Unmarshal([]byte(line), &record); err != nil {
					t.Fatal(err)
				}
				if record["reqTraceId"] != "trace" || record["view"] != m.View || record["rows"] != float64(2) || record["elapsed"] != "3ms" {
					t.Fatalf("missing context: %s", line)
				}
				if i == 0 && record["level"] != "WARN" {
					t.Fatalf("cache failure must warn: %s", line)
				}
				if i == 1 {
					for key, want := range map[string]any{"executionId": "execution", "parentId": "parent", "cache_type": "lazy", "cache_namespace": "metadata", "cache_set": "flights", "cache_error_type": "TIMEOUT", "cache_error_code": float64(9)} {
						if record[key] != want {
							t.Fatalf("missing %s: %s", key, line)
						}
					}
				}
				if i == 2 && (record["metricId"] != "metric" || record["status"] != "error") {
					t.Fatalf("completion context: %s", line)
				}
				if i > 0 {
					want := strings.ReplaceAll(reason, "password: YES", "password: [REDACTED]")
					if record["level"] != "ERROR" || !strings.Contains(record["error"].(string), want) {
						t.Fatalf("reason lost: %s", line)
					}
				}
			}
			for _, private := range []string{"private_literal", "private-bind", "private-key", `"SQL"`, `"args"`} {
				if strings.Contains(output.String(), private) {
					t.Fatalf("leaked %s: %s", private, output.String())
				}
			}
			after, _ := json.Marshal(m)
			if !bytes.Equal(before, after) {
				t.Fatal("source metric mutated")
			}
		})
	}
}

func TestNativePreparationFailureAndAbsentEvidence(t *testing.T) {
	var output bytes.Buffer
	r := NewRecorder(slog.New(slog.NewJSONHandler(&output, nil)))
	r.Read("", &response.Metric{View: "records", Error: "prepare: context deadline exceeded"})
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["error"] != "prepare: context deadline exceeded" || record["reqTraceId"] != "unknown" {
		t.Fatalf("lost preparation failure: %s", output.String())
	}
	output.Reset()
	r.SQL("", "records", nil)
	r.Read("", nil)
	r.SQL("", "records", &response.SQLExecution{})
	var absent *Recorder
	absent.SQL("", "records", &response.SQLExecution{Error: "error"})
	absent.Read("", &response.Metric{Error: "error"})
	if output.Len() != 0 {
		t.Fatal("absent evidence emitted logs")
	}
}

func TestNativeFailurePrivacyAndInjection(t *testing.T) {
	var output bytes.Buffer
	r := NewRecorder(slog.New(slog.NewJSONHandler(&output, nil)))
	message := "connect: connection refused; mysql://reader:url-secret@host/db; reader:dsn-secret@tcp(host:3306)/db; Authorization: Bearer bearer-secret; password='password-secret'; Basic basic-secret; eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJwcml2YXRlIn0.abcdefghijk;\r\nCookie: session=cookie-secret; other=other-secret\r\nbody=body-secret"
	e := &response.SQLExecution{ID: "execution\r\nforged", ParentID: "parent\nforged", Error: message}
	r.SQL("trace\nforged", "records\r\nforged", e)
	r.Read("trace", &response.Metric{View: "records", Error: message})
	for _, private := range []string{"url-secret", "dsn-secret", "bearer-secret", "password-secret", "basic-secret", "eyJhbGci", "cookie-secret", "other-secret", "body-secret"} {
		if strings.Contains(output.String(), private) {
			t.Fatalf("leaked %s: %s", private, output.String())
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(record["error"].(string), "connection refused") {
			t.Fatalf("useful reason lost: %s", line)
		}
		for _, value := range record {
			if text, ok := value.(string); ok && strings.ContainsAny(text, "\r\n") {
				t.Fatalf("record delimiters not escaped: %s", line)
			}
		}
	}
	if e.Error != message {
		t.Fatal("source error mutated")
	}
}

func TestNativeCacheMissAndCancellationAreDistinct(t *testing.T) {
	for _, reason := range []string{"", "read audience: context canceled", "read adOrder: context deadline exceeded"} {
		var output bytes.Buffer
		r := NewRecorder(slog.New(slog.NewJSONHandler(&output, nil)))
		r.SQL("trace", "audience", &response.SQLExecution{Error: reason, CacheStats: &response.CacheStats{Type: "write"}})
		lines := strings.Split(strings.TrimSpace(output.String()), "\n")
		wantCount := 1
		if reason != "" {
			wantCount = 2
		}
		if len(lines) != wantCount {
			t.Fatalf("unexpected miss/cancellation records: %s", output.String())
		}
		for i, line := range lines {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatal(err)
			}
			if record["cache_error_type"] != "" || record["cache_error_code"] != float64(0) || record["found_warmup"] != false || record["found_lazy"] != false {
				t.Fatalf("miss mislabeled: %s", line)
			}
			if i == 0 && (record["level"] != "INFO" || record["type"] != "write") {
				t.Fatalf("normal miss must remain INFO: %s", line)
			}
			if i == 1 && (record["level"] != "ERROR" || record["error"] != reason) {
				t.Fatalf("cancellation reason lost: %s", line)
			}
		}
	}
}

func TestNativePreservesExactMySQLDriverError(t *testing.T) {
	driverErr := &mysql.MySQLError{
		Number:   1146,
		SQLState: [5]byte{'4', '2', 'S', '0', '2'},
		Message:  "Table 'ci_ads.audience' doesn't exist",
	}
	const query = "SELECT * FROM ci_ads.audience WHERE id = ?"
	wrapped := fmt.Errorf("failed to run query: %v, due to %w", query, driverErr)
	for _, message := range []string{driverErr.Error(), wrapped.Error()} {
		var output bytes.Buffer
		r := NewRecorder(slog.New(slog.NewJSONHandler(&output, nil)))
		r.SQL("trace", "audience", &response.SQLExecution{SQL: query, Error: message})
		var record map[string]any
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		want := driverErr.Error()
		if message == wrapped.Error() {
			want = "failed to run query: [REDACTED], due to " + driverErr.Error()
		}
		if record["error"] != want {
			t.Fatalf("driver code, SQLSTATE or message changed: got %q, want %q", record["error"], want)
		}
	}
}
