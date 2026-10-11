package dml

import (
	"context"
	"fmt"
	"strings"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
)

func TestQueueContractSliceIdentityPresencePerRow(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		t.Run(fmt.Sprintf("mutate=%v", mutate), func(t *testing.T) {
			d, _, tx := queueFixture(t, true)
			rows := []*queuePresentID{
				{ID: 7, Name: "allocated", Has: &queuePresentHas{ID: true, Name: true}},
				{ID: 0, Name: "explicit-zero", Has: &queuePresentHas{ID: true, Name: true}},
			}
			if err := d.InsertWithQueueContract("queue_items", rows, rhandler.SourceSlice); err != nil {
				t.Fatal(err)
			}
			if mutate {
				rows[1].Has.ID = false
			}
			err := d.Complete(context.Background(), nil)
			if mutate {
				if err == nil {
					t.Fatal("second row's presence mutation escaped protection")
				}
				queuePrefix(t, tx, 0)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			queuePrefix(t, tx, 2)
			var id int
			if err := tx.QueryRow("SELECT id FROM queue_items WHERE name='explicit-zero'").Scan(&id); err != nil || id != 0 {
				t.Fatal("second row's explicit zero was not preserved", id, err)
			}
		})
	}
	d, _, tx := queueFixture(t, true)
	rows := []*queuePresentID{
		{ID: 7, Name: "allocated", Has: &queuePresentHas{ID: true, Name: true}},
		{ID: 0, Name: "absent-zero", Has: &queuePresentHas{Name: true}},
	}
	err := d.InsertWithQueueContract("queue_items", rows, rhandler.SourceSlice)
	if err == nil || !strings.Contains(err.Error(), "row 1") || len(d.queue) != 0 {
		t.Fatal("invalid second identity must reject the entire slice", err)
	}
	queuePrefix(t, tx, 0)
}

type queueMappingRegionRow struct {
	AdOrderID int `sqlx:"AD_ORDER_ID"`
	RegionID  int `sqlx:"DATACENTER_REGION_ID"`
}

func BenchmarkQueueContractSourceSliceAdmission(b *testing.B) {
	for _, size := range []int{25, 1000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			rows := make([]*queueMappingRegionRow, size)
			for i := range rows {
				rows[i] = &queueMappingRegionRow{AdOrderID: i + 1, RegionID: 1}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d := NewData(nil)
				if err := d.BeginInvocation(); err != nil {
					b.Fatal(err)
				}
				if err := d.InsertWithQueueContract("regions", rows, rhandler.SourceSlice); err != nil {
					b.Fatal(err)
				}
				if len(d.queue) != 1 || len(d.queue[0].payloadEvidence.rows) != size {
					b.Fatal("incomplete source-slice evidence")
				}
			}
		})
	}
}
