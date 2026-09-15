package dml

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/testutil/reservationdb"
)

func TestSequenceLiveProcessWorker(t *testing.T) {
	driver, table := os.Getenv("F1_WORKER_DRIVER"), os.Getenv("F1_WORKER_TABLE")
	if driver == "" {
		t.Skip("subprocess helper")
	}
	h := reservationdb.Join(t, driver)
	h.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	data := NewData(h.DB, WithTx(tx), WithSequenceStrategy(dialect.PresetIDWithReservation))
	if err = data.BeginInvocation(); err != nil {
		t.Fatal(err)
	}
	if err = data.Start(ctx); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("READY")
	if !scanner.Scan() {
		t.Fatal("missing start barrier")
	}
	row := &concurrentSequenceRow{Name: "process"}
	if err = data.Allocate(ctx, table, row, "ID"); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("ALLOC %d\n", row.ID)
	if !scanner.Scan() {
		t.Fatal("missing completion barrier")
	}
	if os.Getenv("F1_RESERVE_ONLY") != "true" {
		if err = data.Insert(table, row); err != nil {
			t.Fatal(err)
		}
	}
	if err = data.Complete(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal("caller could not commit:", err)
	}
}

func TestSequenceLiveProcesses(t *testing.T) {
	for _, driver := range []string{"mysql", "postgres"} {
		for _, reserveOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reserve-only=%v", driver, reserveOnly), func(t *testing.T) {
				h := reservationdb.Open(t, driver)
				h.CreateRecords(t)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				type process struct {
					command *exec.Cmd
					input   io.WriteCloser
					output  *bufio.Scanner
				}
				workers := make([]process, 2)
				for i := range workers {
					cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSequenceLiveProcessWorker$")
					cmd.Env = append(os.Environ(), "F1_WORKER_DRIVER="+driver, "F1_WORKER_TABLE="+h.Table("records"), fmt.Sprintf("F1_RESERVE_ONLY=%v", reserveOnly))
					input, err := cmd.StdinPipe()
					if err != nil {
						t.Fatal(err)
					}
					output, err := cmd.StdoutPipe()
					if err != nil {
						t.Fatal(err)
					}
					cmd.Stderr = os.Stderr
					if err = cmd.Start(); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = cmd.Process.Kill() })
					workers[i] = process{cmd, input, bufio.NewScanner(output)}
				}
				for _, w := range workers {
					if !w.output.Scan() || w.output.Text() != "READY" {
						t.Fatalf("worker failed startup: %s", w.output.Text())
					}
				}
				type allocation struct {
					worker int
					id     int64
					err    error
				}
				allocated := make(chan allocation, 2)
				for i, w := range workers {
					if _, err := fmt.Fprintln(w.input, "start"); err != nil {
						t.Fatal(err)
					}
					go func(i int, w process) {
						if !w.output.Scan() {
							allocated <- allocation{err: fmt.Errorf("worker ended before allocation")}
							return
						}
						id, err := strconv.ParseInt(strings.TrimPrefix(w.output.Text(), "ALLOC "), 10, 64)
						allocated <- allocation{i, id, err}
					}(i, w)
				}
				var first, second allocation
				select {
				case first = <-allocated:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if first.err != nil {
					t.Fatal(first.err)
				}
				if driver == "postgres" {
					select {
					case second = <-allocated:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				} else {
					select {
					case item := <-allocated:
						t.Fatalf("MySQL allocation escaped caller transaction: %+v", item)
					case <-time.After(100 * time.Millisecond):
					}
				}
				if _, err := fmt.Fprintln(workers[first.worker].input, "complete"); err != nil {
					t.Fatal(err)
				}
				if err := workers[first.worker].command.Wait(); err != nil {
					t.Fatal(err)
				}
				if driver == "mysql" {
					select {
					case second = <-allocated:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if second.err != nil || first.id == 0 || second.id == 0 || first.id == second.id {
					t.Fatalf("process reservation: %+v %+v", first, second)
				}
				if _, err := fmt.Fprintln(workers[second.worker].input, "complete"); err != nil {
					t.Fatal(err)
				}
				if err := workers[second.worker].command.Wait(); err != nil {
					t.Fatal(err)
				}
				want := 2
				if reserveOnly {
					want = 0
				}
				if got := h.Count(t, "records"); got != want {
					t.Fatalf("business rows: %d want %d", got, want)
				}
			})
		}
	}
}
