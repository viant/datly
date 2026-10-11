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
	for _, method := range []string{"Insert", "Delete"} {
		t.Run(method, func(t *testing.T) {
			probe := &batchGuardProbe{}
			guard := &mutationGuard{depth: 1}
			capability := dmlCapability{service: probe, guard: guard}
			call := capability.InsertWithBatchSize
			if method == "Delete" {
				call = capability.DeleteWithBatchSize
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
			}
			if err := call("records", nil, 8000, &affected); err == nil {
				t.Fatal("missing optional capability accepted")
			}
		})
	}
}
