package velty

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestInputCaptureConfigurationIsImmutable(t *testing.T) {
	type input struct{ ID int }
	type output struct{ ID int }
	base, err := New[input, output](Config{Template: `#set($Output.ID = $Input.ID)`})
	if err != nil {
		t.Fatal(err)
	}
	configured := base.WithInputCapture(func(_ context.Context, value *input) (any, error) { return value.ID, nil })
	if configured == base || configured.program != base.program {
		t.Fatal("capture configuration must copy handler and reuse immutable program")
	}
	if value, err := base.CaptureInput(context.Background(), &input{ID: 1}); err != nil || value != nil {
		t.Fatalf("base changed: %v %v", value, err)
	}
	var workers sync.WaitGroup
	for id := 0; id < 16; id++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			value, err := configured.CaptureInput(context.Background(), &input{ID: id})
			if err != nil || value != id {
				t.Errorf("capture=%v err=%v", value, err)
			}
		}(id)
	}
	workers.Wait()
	if value, err := configured.WithInputCapture(nil).CaptureInput(context.Background(), &input{}); err != nil || value != nil {
		t.Fatalf("disabled capture=%v err=%v", value, err)
	}
	for _, value := range []any{nil, (*input)(nil), &output{}} {
		if _, err := configured.CaptureInput(context.Background(), value); err == nil {
			t.Fatalf("accepted invalid input %T", value)
		}
	}
	expected := errors.New("capture failed")
	failed := base.WithInputCapture(func(context.Context, *input) (any, error) { return nil, expected })
	if _, err := failed.CaptureInput(context.Background(), &input{}); !errors.Is(err, expected) {
		t.Fatalf("error=%v", err)
	}
}
