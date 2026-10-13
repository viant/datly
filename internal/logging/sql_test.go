package logging

import (
	"bytes"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xdatly/response"
)

// Keep the historical fixture as evidence of legacy display formatting. Server
// diagnostics now retain the redacted reason and never expand positional binds.
func TestLegacyDiagnosticSQLExpansionFormatting(t *testing.T) {
	b, err := os.ReadFile("testdata/original-sql-diagnostics.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]string
	if err := json.Unmarshal(b, &golden); err != nil {
		t.Fatal(err)
	}
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	var sp *string
	var ip *int
	var tp *time.Time
	for _, tc := range []struct {
		name, sql string
		args      []any
	}{
		{"typed_nil", "select ?, ?, ?", []any{sp, ip, tp}},
		{"values", "select ?, ?, ?, ?, ?, ?, ?\nfrom synthetic", []any{7, int64(8), float64(1.234567), true, stamp, "O'Reilly\nline", []byte("bytes")}},
	} {
		expanded := strconv.Quote(strings.ReplaceAll(expandDiagnosticSQL(tc.sql, tc.args), "\n", `\n`))
		if !strings.Contains(golden[tc.name], "sql="+expanded+" params=") {
			t.Fatalf("%s: legacy expansion changed: %s", tc.name, expanded)
		}
	}
	stampPtr := &stamp
	for name, arg := range map[string]any{"expand_double_time_pointer": &stampPtr, "expand_pointer_to_nil_time": &tp} {
		if got := expandDiagnosticSQL("select ?", []any{arg}); got != golden[name] {
			t.Errorf("%s: got %q, want %q", name, got, golden[name])
		}
	}
	if got := expandDiagnosticSQL("select ?", []any{nil}); got != "select  NULL " {
		t.Fatalf("NULL expansion: %q", got)
	}
}

func TestSQLDiagnosticPolicyAndContext(t *testing.T) {
	fixed := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	e := &response.SQLExecution{ID: "execution", ParentID: "parent", SQL: "select id from records where id = ?", Args: []any{"private-bind", "Bearer private-token", nil}, Error: "failed to run query: select id from records where id = ?, due to Error 1142 (42000): SELECT command denied to user 'reader' for table 'records'", Rows: 3, StartTime: fixed, EndTime: fixed.Add(2 * time.Millisecond)}
	stats := &cache.Stats{Type: cache.TypeReadMulti, Namespace: "metadata", Dataset: "records", ErrorType: "TIMEOUT", ErrorCode: 9, WarmupKey: "private-key", MarkerKey: "private-marker", Key: "private-cache-key"}
	before, _ := json.Marshal([]any{e, stats})
	for mask := 0; mask < 8; mask++ {
		var output bytes.Buffer
		if !LogSQL(NewState(NewSink(&output, mask&1 != 0, mask&2 != 0, mask&4 != 0)), "trace", "records", e, stats) {
			t.Fatal("configured diagnostics rejected")
		}
		got := output.String()
		for _, want := range []string{"[WARN] datly cache read", "[ERROR] datly sql", "reqTraceId=trace view=records", "executionId=execution parentId=parent", "rows=3 elapsed=2ms", "cache_error_type=\"TIMEOUT\" cache_error_code=9", "namespace=metadata set=records", "Error 1142 (42000): SELECT command denied", "for table 'records'"} {
			if !strings.Contains(got, want) {
				t.Errorf("profile %d missing %q in %s", mask, want, got)
			}
		}
		for _, private := range []string{"private-bind", "private-token", "private-key", "private-marker", "private-cache-key", " args=", " params="} {
			if strings.Contains(got, private) {
				t.Errorf("profile %d leaked %q", mask, private)
			}
		}
		if sql := strings.Contains(got, ` sql="select id from records where id = ?"`); sql != (mask&4 != 0) {
			t.Errorf("profile %d SQL policy: %s", mask, got)
		}
		if strings.Count(got, "\n") != 2 || strings.Index(got, "datly cache read") > strings.Index(got, "datly sql") {
			t.Fatalf("invalid record order/count: %s", got)
		}
	}
	after, _ := json.Marshal([]any{e, stats})
	if !bytes.Equal(before, after) {
		t.Fatal("logging mutated evidence")
	}
}

func TestSQLDiagnosticAbsentAndCacheFallback(t *testing.T) {
	var output bytes.Buffer
	s := NewSink(&output, false, false, false)
	s.sqlRead("", "records", nil, nil)
	s.sqlRead("", "records", &response.SQLExecution{SQL: "select ?", Args: []any{7}}, nil)
	if output.Len() != 0 || LogSQL(nil, "", "records", &response.SQLExecution{Error: "synthetic"}, nil) || LogSQL(NewState(nil), "", "records", nil, nil) {
		t.Fatal("absent evidence/owner emitted diagnostics")
	}
	s.sqlRead("", "records", &response.SQLExecution{Error: "context canceled", CacheStats: &response.CacheStats{ErrorType: "TIMEOUT", ErrorCode: 9}}, nil)
	if got := output.String(); !strings.Contains(got, `error="context canceled"`) || !strings.Contains(got, `cache_error_code=9`) || !strings.Contains(got, "reqTraceId=unknown") {
		t.Fatalf("missing fallback evidence: %s", got)
	}
}

func TestCompatibilityReadPreparationFailure(t *testing.T) {
	var output bytes.Buffer
	m := &response.Metric{ID: "metric", View: "records", Error: "prepare: dial tcp 127.0.0.1:3306: connect: connection refused", Rows: 0, Elapsed: "1ms"}
	NewSink(&output, false, false, false).Read("trace", m)
	for _, want := range []string{"[ERROR] datly view read", "metricId=metric", "status=error", "prepare: dial tcp 127.0.0.1:3306: connect: connection refused"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("missing %s: %s", want, output.String())
		}
	}
}

func TestCompatibilityCacheMissIsNotCacheFailure(t *testing.T) {
	var output bytes.Buffer
	NewSink(&output, false, false, false).sqlRead("trace", "audience", &response.SQLExecution{Error: "read audience: context canceled"}, &cache.Stats{Type: cache.TypeWrite})
	got := output.String()
	for _, want := range []string{"[INFO] datly cache read", "source=miss_write type=write", `cache_error_type="" cache_error_code=0`, `error="read audience: context canceled"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s: %s", want, got)
		}
	}
	if strings.Contains(got, "[WARN]") {
		t.Fatalf("miss mislabeled as cache failure: %s", got)
	}
}
