package locator

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hstate "github.com/viant/xdatly/handler/state"
)

func TestWithFormClonesInputValues(t *testing.T) {
	form := hstate.NewForm()
	form.Set("id", "1", "2")

	options := NewOptions([]Option{WithForm(form)})
	form.Set("id", "mutated")
	form.Add("new", "ignored")

	values, ok := options.Form.Lookup("id")
	if !ok {
		t.Fatal("expected cloned id values")
	}
	if got, want := fmt.Sprint(values), "[1 2]"; got != want {
		t.Fatalf("cloned id values=%s want %s", got, want)
	}
	if _, ok = options.Form.Lookup("new"); ok {
		t.Fatal("cloned form changed when source form mutated")
	}
}

func TestWithFormMergesIntoPrivateCopy(t *testing.T) {
	first := hstate.NewForm()
	first.Set("a", "1")
	second := hstate.NewForm()
	second.Set("b", "2")

	options := NewOptions([]Option{WithForm(first), WithForm(second)})
	first.Set("a", "mutated")
	second.Set("b", "mutated")

	for key, want := range map[string]string{"a": "1", "b": "2"} {
		if got := options.Form.Get(key); got != want {
			t.Fatalf("form[%s]=%q want %q", key, got, want)
		}
	}
}

func TestWithFormConcurrentSourceMutationAndLookup(t *testing.T) {
	source := hstate.NewForm()
	source.Set("id", "1")
	options := NewOptions([]Option{WithForm(source)})
	locator := &Form{form: options.Form}

	deadline := time.Now().Add(500 * time.Millisecond)
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			source.Add("id", fmt.Sprintf("%d", next.Add(1)))
		}
	}()

	go func() {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			_, _, err := locator.Value(context.Background(), reflect.TypeOf([]string{}), "id")
			if err != nil {
				t.Errorf("Value() error: %v", err)
				return
			}
		}
	}()

	waitGroup.Wait()
}

func TestFormValueConcurrentMutationCopiesValues(t *testing.T) {
	form := hstate.NewForm()
	form.Set("id", "1")
	locator := &Form{form: form}

	deadline := time.Now().Add(500 * time.Millisecond)
	var next atomic.Uint64
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			form.Add("id", fmt.Sprintf("%d", next.Add(1)))
		}
	}()

	go func() {
		defer waitGroup.Done()
		for time.Now().Before(deadline) {
			_, _, err := locator.Value(context.Background(), reflect.TypeOf([]string{}), "id")
			if err != nil {
				t.Errorf("Value() error: %v", err)
				return
			}
		}
	}()

	waitGroup.Wait()
}
