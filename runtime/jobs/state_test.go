package jobs_test

import (
	"context"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"reflect"
	"strings"
	"testing"
)

type presence struct{ ID, Text bool }
type stateInput struct {
	ID   int
	Text string
	Has  *presence `setMarker:"true"`
}

func TestOriginalStateRetainsCanonicalPresence(t *testing.T) {
	required := true
	bindings := []bindly.BindingSpec{{Name: "RecordID", Path: "ID", Required: &required, Location: bindstate.Location{Kind: "query", In: "id"}}, {Name: "Text", Path: "Text", Location: bindstate.Location{Kind: "query", In: "text"}}}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeOf(stateInput{}), bindings...)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := plan.Projection()
	if err != nil {
		t.Fatal(err)
	}
	ref := spec.RouteRef{Method: "GET", Path: "/state"}
	contract, err := registry.NewInputContract(reflect.TypeOf(stateInput{}), projection, registry.RouteInput{Route: ref, Plan: plan, Bindings: bindings})
	if err != nil {
		t.Fatal(err)
	}
	route, _ := contract.ForRoute(ref)
	codec, err := jobs.NewStateCodec(route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(&stateInput{ID: 7}); err == nil {
		t.Fatal("missing required presence accepted during capture")
	}
	encoded, err := codec.Encode(&stateInput{ID: 7, Text: "not present", Has: &presence{ID: true}})
	if err != nil {
		t.Fatal(err)
	}
	if encoded != `{"RecordID":7}` {
		t.Fatalf("state=%s", encoded)
	}
	restored, err := codec.Decode(`{"RecordID":7,"Query":"original cached query"}`)
	if err != nil {
		t.Fatal(err)
	}
	actual := &stateInput{}
	if err := injector.Bind(context.Background(), actual, bindly.WithPlan(plan), bindly.WithReplay(bindly.ReplayBinding{Replay: restored, Only: true})); err != nil {
		t.Fatal(err)
	}
	if actual.Has == nil || !actual.Has.ID || actual.Has.Text || actual.Text != "" {
		t.Fatalf("presence=%+v", actual)
	}
	for _, state := range []string{`{}`, `{"RecordID":null}`, `{"RecordID":"wrong"}`, `null`, `[]`} {
		replay, err := codec.Decode(state)
		if err == nil {
			err = injector.Bind(context.Background(), &stateInput{}, bindly.WithPlan(plan), bindly.WithReplay(bindly.ReplayBinding{Replay: replay, Only: true}))
		}
		if err == nil {
			t.Errorf("invalid state accepted: %s", state)
		}
	}
	if _, err := codec.Encode(&stateInput{ID: 7, Text: strings.Repeat("x", jobs.MaxInlineState), Has: &presence{ID: true, Text: true}}); err == nil {
		t.Fatal("oversize state was accepted")
	}
}
