package dml

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestSequenceProcessWorker(t *testing.T) {
	dsn := os.Getenv("DATLY_F1_WORKER_DSN")
	if dsn == "" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := sqlite.New(t, sqlite.WithDSN(dsn))
	h.DB.SetMaxOpenConns(1)
	data := NewData(h.DB)
	if err := data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err := data.Start(ctx); err != nil {
		t.Fatal(err)
	}
	input := bufio.NewScanner(os.Stdin)
	fmt.Println("READY")
	if !input.Scan() {
		t.Fatal("missing start barrier")
	}
	row := &concurrentSequenceRow{Name: "process"}
	if err := data.Allocate(ctx, "records", row, "ID"); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("ALLOC %d\n", row.ID)
	if !input.Scan() {
		t.Fatal("missing queue barrier")
	}
	if os.Getenv("DATLY_F1_WORKER_RESERVE_ONLY") != "true" {
		if err := data.Insert("records", row); err != nil {
			t.Fatal(err)
		}
	}
	if err := data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSequenceConcurrentProcesses(t *testing.T) {
	for _, journal := range []string{"DELETE", "WAL"} {
		for _, schema := range sequenceSchemas {
			for _, reserveOnly := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/reserve-only=%v", journal, schema.name, reserveOnly), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					dsn := filepath.Join(t.TempDir(), "process.db") + "?_busy_timeout=5000"
					h := sqlite.New(t, sqlite.WithDSN(dsn))
					if err := h.ExecStatements(ctx, "PRAGMA journal_mode="+journal, schema.ddl); err != nil {
						t.Fatal(err)
					}
					type worker struct {
						cmd *exec.Cmd
						in  io.WriteCloser
						out *bufio.Scanner
					}
					workers := make([]worker, 2)
					for i := range workers {
						cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSequenceProcessWorker$")
						cmd.Env = append(os.Environ(), "DATLY_F1_WORKER_DSN="+dsn, fmt.Sprintf("DATLY_F1_WORKER_RESERVE_ONLY=%v", reserveOnly))
						in, err := cmd.StdinPipe()
						if err != nil {
							t.Fatal(err)
						}
						out, err := cmd.StdoutPipe()
						if err != nil {
							t.Fatal(err)
						}
						cmd.Stderr = os.Stderr
						if err = cmd.Start(); err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = cmd.Process.Kill() })
						workers[i] = worker{cmd, in, bufio.NewScanner(out)}
					}
					for _, w := range workers {
						if !w.out.Scan() || w.out.Text() != "READY" {
							t.Fatalf("worker failed to begin: %s", w.out.Text())
						}
					}
					type allocation struct {
						worker int
						id     int64
						err    error
					}
					results := make(chan allocation, 2)
					for i, w := range workers {
						if _, err := fmt.Fprintln(w.in, "start"); err != nil {
							t.Fatal(err)
						}
						go func(i int, w worker) {
							if !w.out.Scan() {
								results <- allocation{err: fmt.Errorf("worker ended before allocation")}
								return
							}
							line := w.out.Text()
							id, err := strconv.ParseInt(strings.TrimPrefix(line, "ALLOC "), 10, 64)
							results <- allocation{i, id, err}
						}(i, w)
					}
					var first allocation
					select {
					case first = <-results:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if first.err != nil || first.id == 0 {
						t.Fatalf("first allocation: %+v", first)
					}
					select {
					case second := <-results:
						t.Fatalf("second process escaped transaction barrier: %+v", second)
					case <-time.After(100 * time.Millisecond):
					}
					if _, err := fmt.Fprintln(workers[first.worker].in, "queue"); err != nil {
						t.Fatal(err)
					}
					if err := workers[first.worker].cmd.Wait(); err != nil {
						t.Fatal(err)
					}
					var second allocation
					select {
					case second = <-results:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if second.err != nil || second.id == 0 || second.id == first.id {
						t.Fatalf("process allocations: %+v %+v", first, second)
					}
					if _, err := fmt.Fprintln(workers[second.worker].in, "queue"); err != nil {
						t.Fatal(err)
					}
					if err := workers[second.worker].cmd.Wait(); err != nil {
						t.Fatal(err)
					}
					var n int
					want := 2
					if reserveOnly {
						want = 0
					}
					if err := h.DB.QueryRowContext(ctx, "SELECT count(DISTINCT id) FROM records").Scan(&n); err != nil || n != want {
						t.Fatalf("process persistence: %d %v", n, err)
					}
				})
			}
		}

	}
}
