package extension

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/xdatly/predicate"
)

func TestNewDurationPredicate_DoesNotUseLogicalOrInVelty(t *testing.T) {
	predicate := NewDurationPredicate()
	if predicate == nil || predicate.Template == nil {
		t.Fatalf("expected duration predicate template")
	}
	if strings.Contains(predicate.Template.Source, "||") {
		t.Fatalf("expected duration predicate template to avoid logical OR, got:\n%s", predicate.Template.Source)
	}
}

func TestNewNopPredicateProducesEmptyTemplate(t *testing.T) {
	predicate := NewNopPredicate()
	if predicate == nil || predicate.Template == nil {
		t.Fatalf("expected nop predicate template")
	}
	if predicate.Template.Name != PredicateNop || predicate.Template.Source != "" {
		t.Fatalf("unexpected nop predicate: %#v", predicate.Template)
	}
}

func TestDefaultExtensionRegistersNopPredicate(t *testing.T) {
	predicate, err := Config.Predicates.Lookup(PredicateNop)
	if err != nil {
		t.Fatal(err)
	}
	if predicate == nil || predicate.Template == nil || predicate.Template.Source != "" {
		t.Fatalf("unexpected registered nop predicate: %#v", predicate)
	}
}

// TestPredicateRegistryConcurrentLookupAndAdd reproduces concurrent map access
// on PredicateRegistry.registry. Lookup reads while Add writes, and the
// embedded mutex is unused.
func TestPredicateRegistryConcurrentLookupAndAdd(t *testing.T) {
	deadline := time.Now().Add(3 * time.Second)
	registry := NewPredicates()
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(5)

	run := func(fn func()) {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			fn()
		}
	}

	go run(func() {
		name := fmt.Sprintf("pred%d", next.Add(1))
		registry.Add(&predicate.Template{Name: name, Source: "1 = 1"})
	})
	for worker := 0; worker < 4; worker++ {
		go run(func() {
			_, _ = registry.Lookup(fmt.Sprintf("pred%d", next.Add(1)))
		})
	}
	waitGroup.Wait()
}
