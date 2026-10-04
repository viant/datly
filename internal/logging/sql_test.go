package logging

import (
	"bytes"
	"encoding/json"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/xdatly/response"
	"os"
	"testing"
	"time"
)

// Fixture bytes were captured from Datly 69de0137b44472faf0671525d903bc4a65eb230b.
// The original oracle and its byte-verified source provenance remain in
// /var/folders/9q/hhl5ms253kz60h0scsqlnn6h0000gp/T/plan10-sql-cache-oracle-vvusdt0q/datly.
func TestSQLPinnedOriginalDiagnostics(t *testing.T) {
	b, err := os.ReadFile("testdata/original-sql-diagnostics.json")
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]string
	if err = json.Unmarshal(b, &golden); err != nil {
		t.Fatal(err)
	}
	type testCase struct {
		name, sql, err string
		args           []any
		stats          *cache.Stats
	}
	cases := []testCase{}
	for _, msg := range []string{"ordinary\nerror", "outer, due to middle, due to  final  ", "outer due to  final  ", "driver failed to run query: select hidden", "failed to run query: select hidden", "outer, due to comma due to plain"} {
		cases = append(cases, testCase{name: "error_" + msg, sql: "select ?\nfrom x", err: msg, args: []any{"O'Reilly"}})
	}
	var sp *string
	var ip *int
	var tp *time.Time
	cases = append(cases, testCase{name: "typed_nil", sql: "select ?, ?, ?", err: "synthetic", args: []any{sp, ip, tp}}, testCase{name: "values", sql: "select ?, ?, ?, ?, ?, ?, ?\nfrom synthetic", err: "synthetic", args: []any{7, int64(8), float64(1.234567), true, time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), "O'Reilly\nline", []byte("bytes")}})
	for _, c := range []struct {
		name  string
		stats *cache.Stats
	}{{"nil", nil}, {"miss", &cache.Stats{}}, {"warmup", &cache.Stats{Type: cache.TypeReadMulti, FoundWarmup: true, WarmupKey: "synthetic-warmup", MarkerKey: "synthetic-marker"}}, {"lazy", &cache.Stats{Type: cache.TypeReadSingle, FoundLazy: true}}, {"write", &cache.Stats{Type: cache.TypeWrite}}, {"error", &cache.Stats{Type: cache.TypeReadMulti, ErrorType: "synthetic"}}, {"warmup_fallback", &cache.Stats{FoundWarmup: true, FoundLazy: true}}, {"lazy_fallback", &cache.Stats{FoundLazy: true}}, {"marker_only", &cache.Stats{MarkerKey: "synthetic-marker"}}} {
		if c.stats != nil {
			c.stats.Namespace = "synthetic-ns"
			c.stats.Dataset = "synthetic-set"
			c.stats.RecordsCounter = 12
		}
		cases = append(cases, testCase{name: "cache_" + c.name, args: []any{7, "synthetic"}, stats: c.stats})
	}
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	stampPtr := &stamp
	for name, arg := range map[string]any{"expand_double_time_pointer": &stampPtr, "expand_pointer_to_nil_time": &tp} {
		if got := expandDiagnosticSQL("select ?", []any{arg}); got != golden[name] {
			t.Errorf("%s: got %q, want %q", name, got, golden[name])
		}
	}
	fixed := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want, ok := golden[tc.name]
			if !ok {
				t.Fatal("missing pinned golden")
			}
			for mask := 0; mask < 8; mask++ {
				var output bytes.Buffer
				sink := NewSink(&output, mask&1 != 0, mask&2 != 0, mask&4 != 0)
				e := &response.SQLExecution{SQL: tc.sql, Args: tc.args, Error: tc.err, Rows: 3, StartTime: fixed, EndTime: fixed.Add(1234567 * time.Nanosecond)}
				if !LogSQL(NewState(sink), "synthetic-trace", "Synthetic", e, tc.stats) {
					t.Fatal("configured owner rejected diagnostics")
				}
				if output.String() != want {
					t.Errorf("profile %d differs:\ngot %q\nwant %q", mask, output.String(), want)
				}
			}
		})
	}
}

func TestSQLDiagnosticNullAndAbsentEvidence(t *testing.T) {
	// Original nil-interface behavior panicked. NULL is an intentional safe fix.
	if got := expandDiagnosticSQL("select ?", []any{nil}); got != "select  NULL " {
		t.Fatalf("NULL expansion: %q", got)
	}
	var output bytes.Buffer
	s := NewSink(&output, false, false, false)
	s.sqlRead("", "Synthetic", &response.SQLExecution{SQL: "select ?", Args: []any{nil}, Error: "synthetic"}, nil)
	want := "[ERROR] datly sql reqTraceId=unknown view=Synthetic error=\"synthetic\" sql=\"select  NULL \" params=[<nil>]\n"
	if output.String() != want {
		t.Fatalf("safe NULL diagnostic: %q", output.String())
	}
	output.Reset()
	s.sqlRead("", "Synthetic", nil, nil)
	s.sqlRead("", "Synthetic", &response.SQLExecution{SQL: "select ?", Args: []any{7}}, nil)
	if output.Len() != 0 {
		t.Fatal("absent/error-free evidence emitted a diagnostic")
	}
	if LogSQL(nil, "", "Synthetic", &response.SQLExecution{Error: "synthetic"}, nil) {
		t.Fatal("unconfigured diagnostics enabled")
	}
	if normalizeDatabaseError("") != "" {
		t.Fatal("nil-error normalization differs")
	}
}

func TestSQLCacheBeforeError(t *testing.T) {
	b, err := os.ReadFile("testdata/original-sql-diagnostics.json")
	if err != nil {
		t.Fatal(err)
	}
	var g map[string]string
	if err = json.Unmarshal(b, &g); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	fixed := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	e := &response.SQLExecution{SQL: "select ?\nfrom x", Args: []any{"O'Reilly"}, Error: "ordinary\nerror", Rows: 3, StartTime: fixed, EndTime: fixed.Add(1234567 * time.Nanosecond)}
	stats := &cache.Stats{Type: cache.TypeReadMulti, FoundWarmup: true, RecordsCounter: 12, Namespace: "synthetic-ns", Dataset: "synthetic-set", WarmupKey: "synthetic-warmup", MarkerKey: "synthetic-marker"}
	NewSink(&output, false, false, false).sqlRead("synthetic-trace", "Synthetic", e, stats)
	// Cache fixture uses different synthetic args; replace only its literal args.
	cacheLine := bytes.ReplaceAll([]byte(g["cache_warmup"]), []byte("args=[7 synthetic]"), []byte("args=[O'Reilly]"))
	if want := string(cacheLine) + g["error_ordinary\nerror"]; output.String() != want {
		t.Fatalf("ordering differs: %q", output.String())
	}
}
