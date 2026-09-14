package tag

import (
	"fmt"
	"testing"
)

func TestViewBatchConcurrencyRoundTrip(t *testing.T) {
	for _, concurrency := range []int{0, 1, 2, 8} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			value := fmt.Sprintf("items,batch=25,batchConcurrency=%d,relationalConcurrency=3,partitioner=ItemPartitioner,concurrency=4", concurrency)
			view, err := ParseView(value)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := view.Value()
			if err != nil {
				t.Fatal(err)
			}
			actual, err := ParseView(encoded)
			if err != nil || actual == nil || actual.Batch != 25 || actual.BatchConcurrency != concurrency ||
				actual.RelationalConcurrency != 3 || actual.Partitioning == nil || actual.Partitioning.Concurrency != 4 {
				t.Fatalf("batch/callback/partition settings = %+v, error=%v", actual, err)
			}
		})
	}
	for _, invalid := range []string{"-1", "1.5", "many", "999999999999999999999999"} {
		if _, err := ParseView("items,batchConcurrency=" + invalid); err == nil {
			t.Errorf("accepted invalid concurrency %q", invalid)
		}
	}
	if _, err := (View{Name: "items", BatchConcurrency: -1}).Value(); err == nil {
		t.Fatal("formatter silently discarded negative concurrency")
	}
}
