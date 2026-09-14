package dml

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	xexec "github.com/viant/xdatly/exec"
	"testing"
)

func BenchmarkParityDML(b *testing.B) {
	for _, scope := range []string{"native-call", "flush"} {
		for _, capture := range []bool{false, true} {
			label := "no-context"
			if capture {
				label = "capture"
			}
			b.Run(scope+"/"+label, func(b *testing.B) {
				h := sqlite.New(b, sqlite.WithDSN(":memory:"))
				h.DB.SetMaxOpenConns(1)
				ctx := context.Background()
				ec := &xexec.Context{}
				if capture {
					ctx = xexec.WithContext(ctx, ec)
				}
				if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'row')"); err != nil {
					b.Fatal(err)
				}
				type row struct {
					ID   int    `sqlx:"id,primaryKey"`
					Name string `sqlx:"name"`
				}
				value := &row{ID: 1, Name: "row"}
				d := NewData(h.DB)
				if scope == "native-call" {
					// Warm the same native SQLX service and hold one transaction in both builds.
					if _, err := d.updater(ctx, h.DB, "records"); err != nil {
						b.Fatal(err)
					}
					tx, err := h.DB.BeginTx(ctx, nil)
					if err != nil {
						b.Fatal(err)
					}
					defer tx.Rollback()
					step := executionStep{kind: dataOpUpdate, table: "records", operations: []*dataOperation{{kind: dataOpUpdate, table: "records", data: value}}}
					if err := d.executeUpdateStep(ctx, h.DB, tx, step); err != nil {
						b.Fatal(err)
					}
					ec.Metrics = ec.Metrics[:0]
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if err := d.executeUpdateStep(ctx, h.DB, tx, step); err != nil {
							b.Fatal(err)
						}
						ec.Metrics = ec.Metrics[:0]
					}
				} else {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						// New invocation data, journal, native service setup and real memory-DB commit.
						d := NewData(h.DB)
						if err := d.Update("records", value); err != nil {
							b.Fatal(err)
						}
						if err := d.Flush(ctx, ""); err != nil {
							b.Fatal(err)
						}
						ec.Metrics = ec.Metrics[:0]
					}
				}
			})
		}
	}
}
