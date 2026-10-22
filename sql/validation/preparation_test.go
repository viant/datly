package validation

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	xhandler "github.com/viant/xdatly/handler"
)

func TestCompactCoverageFreshnessAndIsolation(t *testing.T) {
	p := &plan{fieldIndexes: map[string]int{}}
	for i := range 150 {
		name := fmt.Sprintf("Field%d", i)
		p.fields = append(p.fields, name)
		p.fieldIndexes[name] = i
	}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fields := preparationFields{}
			for i, name := range p.fields {
				fields[name] = i%8 == worker
			}
			before := p.snapshot(fields)
			for _, name := range p.fields {
				fields[name] = !fields[name]
			}
			after := p.snapshot(fields)
			for i, name := range p.fields {
				if before.Has(name) != (i%8 == worker) || after.Has(name) != (i%8 != worker) {
					t.Errorf("stale/shared coverage at %s", name)
				}
			}
			if before.Has("Unknown") || after.Has("Unknown") || p.snapshot(nil) != nil {
				t.Error("unknown or nil coverage changed")
			}
		}()
	}
	wg.Wait()
}

type preparationSource struct{ db *sql.DB }

func (s preparationSource) ValidationConnection(context.Context, string) (Connection, error) {
	return Connection{DB: s.db}, nil
}

func TestValidationPhasesRefreshValuesPresenceAndProvenance(t *testing.T) {
	type row struct {
		ID    int  `sqlx:"id,primaryKey"`
		Value *int `sqlx:"value,required"`
	}
	h := sqlite.New(t)
	s := New(preparationSource{db: h.DB})
	one, disabled := 1, false
	rows := []*row{{ID: 1, Value: &one}}
	fields, previous := preparationFields{"Value": true}, preparationFields{"ID": true}
	policies := []xhandler.ValidationOptions{{Action: xhandler.WriteUpdate, Shallow: true, Location: "Rows[0]", Fields: fields, PreviousFields: previous, Previous: &row{ID: 1}, CheckUnique: &disabled, CheckRef: &disabled}}
	validate := func() *xhandler.Validation {
		t.Helper()
		result, err := s.Validate(t.Context(), rows, policies)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if validate().Failed {
		t.Fatal("initial value rejected")
	}
	rows[0].Value = nil
	if result := validate(); !result.Failed || len(result.Violations) != 1 || result.Violations[0].Location != "Rows[0].Value" {
		t.Fatalf("new value or diagnostic not observed: %+v", result)
	}
	fields["Value"] = false
	if validate().Failed {
		t.Fatal("new presence not observed")
	}
	previous["ID"] = false
	if _, err := s.Validate(t.Context(), rows, policies); err == nil || err.Error() != "validation candidate 0: previous identity field ID was not loaded" {
		t.Fatalf("new provenance not observed: %v", err)
	}
	// A shared service may reuse schema, never another invocation's coverage.
	concurrent := New(preparationSource{db: h.DB})
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			current := &row{ID: worker + 1}
			if worker%2 == 0 {
				current.Value = &one
			}
			options := xhandler.ValidationOptions{Action: xhandler.WriteUpdate, Shallow: true, Fields: preparationFields{"Value": true}, PreviousFields: preparationFields{"ID": true}, Previous: &row{ID: worker + 1}, CheckUnique: &disabled, CheckRef: &disabled}
			for range 5 {
				result, err := concurrent.Validate(t.Context(), []*row{current}, []xhandler.ValidationOptions{options})
				if err != nil || result == nil || result.Failed != (worker%2 == 1) {
					t.Errorf("invocation %d leaked validation state: %+v %v", worker, result, err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestDeferredBatchLocationMatchesEagerMap(t *testing.T) {
	batch := &validationBatch{compiled: &plan{locationFields: map[string]bool{"ID": true, "Name": true}}}
	eager := map[string]string{}
	for i := range 12 {
		location := fmt.Sprintf("Rows[%d].Children[1]", i)
		batch.policies = append(batch.policies, xhandler.ValidationOptions{Location: location})
		for field := range batch.compiled.locationFields {
			eager[fmt.Sprintf("_batch[%d].%s", i, field)] = location + "." + field
		}
	}
	for raw, want := range eager {
		got, ok := batch.location(raw)
		if !ok || got != want {
			t.Fatalf("%s: got %q,%v want %q", raw, got, ok, want)
		}
	}
	for _, raw := range []string{"", "_batch[0]", "_batch[].ID", "_batch[-1].ID", "_batch[+1].ID", "_batch[01].ID", "_batch[00].ID", "_batch[ 1].ID", "_batch[1 ].ID", "_batch[12].ID", "_batch[999999999999999999999999].ID", "_batch[1].", "_batch[1].Missing", "_batch[1].Name.Child", "_batch[1].Name ", " _batch[1].ID", "_batch[1]..ID", "other[0].ID"} {
		_, want := eager[raw]
		if _, ok := batch.location(raw); ok != want {
			t.Fatalf("unexpected acceptance: %q", raw)
		}
	}
}
