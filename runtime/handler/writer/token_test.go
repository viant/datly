package writer

import (
	"reflect"
	"testing"
	"time"
)

func TestConcurrencyTokenSnapshotAndEquality(t *testing.T) {
	value := 4
	original := cloneTokenValue(reflect.ValueOf(&value))
	value = 5
	if got := *original.Interface().(*int); got != 4 {
		t.Fatalf("captured pointer token changed with working value: %d", got)
	}
	if !concurrencyTokenEqual(original.Interface(), 4) || concurrencyTokenEqual(original.Interface(), 5) {
		t.Fatal("numeric token comparison lost original scalar value")
	}
	instant := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	shifted := instant.In(time.FixedZone("same instant", 2*60*60))
	if !concurrencyTokenEqual(&instant, shifted) || !concurrencyTokenEqual(instant, &shifted) {
		t.Fatal("same instant in different zones conflicted")
	}
	if concurrencyTokenEqual(&instant, instant.Add(time.Second)) {
		t.Fatal("different instants were accepted")
	}
	status := "accepted"
	captured := cloneTokenValue(reflect.ValueOf(&status))
	status = "running"
	if !concurrencyTokenEqual(captured.Interface(), "accepted") || concurrencyTokenEqual(captured.Interface(), status) {
		t.Fatal("string token comparison lost the captured state")
	}
}
