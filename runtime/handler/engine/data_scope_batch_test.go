package engine

import (
	"errors"
	"testing"
)

type batchGuardProbe struct{ guardProbe }

func (p *batchGuardProbe) InsertWithBatchSize(string, any, int, *int64) error {
	p.mutations++
	return nil
}
func (p *batchGuardProbe) DeleteWithBatchSize(string, any, int, *int64) error {
	p.mutations++
	return nil
}

func TestBatchCapabilityHonorsMutationGuard(t *testing.T) {
	for _, method := range []string{"Insert", "Delete", "InsertIgnore"} {
		t.Run(method, func(t *testing.T) {
			probe := &batchGuardProbe{}
			guard := &mutationGuard{depth: 1}
			capability := dmlCapability{service: probe, guard: guard}
			call := capability.InsertWithBatchSize
			if method == "Delete" {
				call = capability.DeleteWithBatchSize
			} else if method == "InsertIgnore" {
				call = capability.InsertIgnoreWithBatchSize
			}
			var affected int64
			if err := call("records", nil, 8000, &affected); !errors.Is(err, ErrWriteEligibilityMutation) {
				t.Fatalf("guard error=%v", err)
			}
			if probe.mutations != 0 {
				t.Fatal("guarded batch reached service")
			}
			guard.depth = 0
			if err := call("records", nil, 8000, &affected); err != nil {
				t.Fatal(err)
			}
			if probe.mutations != 1 {
				t.Fatal("batch was not delegated exactly once")
			}
			legacy := dmlCapability{service: &guardProbe{}}
			call = legacy.InsertWithBatchSize
			if method == "Delete" {
				call = legacy.DeleteWithBatchSize
			} else if method == "InsertIgnore" {
				call = legacy.InsertIgnoreWithBatchSize
			}
			if err := call("records", nil, 8000, &affected); err == nil {
				t.Fatal("missing optional capability accepted")
			}
		})
	}
}

func (p *batchGuardProbe) InsertWithBatchSizeAndOnDuplicateKeySQL(string, any, int, *int64, string) error {
	p.mutations++
	return nil
}
func TestBatchConflictCapabilityHonorsMutationGuard(t *testing.T) {
	probe := &batchGuardProbe{}
	guard := &mutationGuard{depth: 1}
	c := dmlCapability{service: probe, guard: guard}
	var affected int64
	if err := c.InsertWithBatchSizeAndOnDuplicateKeySQL("records", nil, 137, &affected, "suffix"); !errors.Is(err, ErrWriteEligibilityMutation) {
		t.Fatalf("guard error=%v", err)
	}
	if probe.mutations != 0 {
		t.Fatal("guarded capability reached service")
	}
	guard.depth = 0
	if err := c.InsertWithBatchSizeAndOnDuplicateKeySQL("records", nil, 137, &affected, "suffix"); err != nil {
		t.Fatal(err)
	}
	if probe.mutations != 1 {
		t.Fatal("capability not delegated once")
	}
	if err := (dmlCapability{service: &guardProbe{}}).InsertWithBatchSizeAndOnDuplicateKeySQL("records", nil, 137, &affected, "suffix"); err == nil {
		t.Fatal("missing optional capability accepted")
	}
}

func (p *batchGuardProbe) InsertIgnoreWithBatchSize(string, any, int, *int64) error {
	p.mutations++
	return nil
}

type ignoreForwardProbe struct {
	guardProbe
	table    string
	rows     any
	size     int
	affected *int64
	err      error
}

func (p *ignoreForwardProbe) InsertIgnoreWithBatchSize(table string, rows any, size int, affected *int64) error {
	p.table, p.rows, p.size, p.affected = table, rows, size, affected
	return p.err
}
func TestInsertIgnoreCapabilityPreservesArgumentsAndError(t *testing.T) {
	rows := &[]int{1, 2}
	var affected int64
	wantErr := errors.New("original insert failure")
	probe := &ignoreForwardProbe{err: wantErr}
	err := (dmlCapability{service: probe}).InsertIgnoreWithBatchSize("links", rows, 2, &affected)
	if !errors.Is(err, wantErr) || probe.table != "links" || probe.rows != rows || probe.size != 2 || probe.affected != &affected {
		t.Fatalf("forwarding changed: %v %+v", err, probe)
	}
}
