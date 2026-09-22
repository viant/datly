package engine

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/sql/reader/readmeta"
	xhandler "github.com/viant/xdatly/handler"
)

type metadataTestProjection struct{ identity int }

func (*metadataTestProjection) RootHolder() string { return "" }
func (*metadataTestProjection) DirectOutput() bool { return true }
func (*metadataTestProjection) Fields(int, ...xhandler.ReadStep) (xhandler.FieldSet, error) {
	return readmeta.NewFields(reflect.TypeOf(struct{ ID int }{}), [][]int{{0}}), nil
}

type metadataLifecycleInput struct {
	Rows        []int `bind:"kind=metadataFixture"`
	initialized bool
}

func (i *metadataLifecycleInput) Init(ctx context.Context) error {
	metadata, ok := xhandler.ReadMetadataFromContext(ctx)
	if !ok {
		return fmt.Errorf("metadata missing in Init")
	}
	if _, err := metadata.Projection("Rows"); err != nil {
		return err
	}
	i.initialized = true
	return nil
}

type metadataLifecycleContract struct{}

func (*metadataLifecycleContract) RequiresReadMetadata() bool { return true }
func (*metadataLifecycleContract) CaptureInput(ctx context.Context, input *metadataLifecycleInput) (any, error) {
	if input.initialized {
		return nil, fmt.Errorf("capture followed Init")
	}
	metadata, ok := xhandler.ReadMetadataFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("capture has no metadata")
	}
	_, err := metadata.Projection("Rows")
	return metadata, err
}
func (*metadataLifecycleContract) Exec(ctx context.Context, session xhandler.Session, input *metadataLifecycleInput, output *bool) error {
	metadata, ok := xhandler.ReadMetadataFromContext(ctx)
	if !ok || !input.initialized {
		return fmt.Errorf("metadata or initialization missing")
	}
	value, found, err := session.Binder().Lookup(ctx, xhandler.ReadMetadataKey)
	if err != nil {
		return err
	}
	if !found || value != metadata {
		return fmt.Errorf("DI and context evidence differ")
	}
	snapshot, found, err := session.Binder().Lookup(ctx, xhandler.InputSnapshotKey)
	if err != nil {
		return err
	}
	if !found || snapshot != metadata {
		return fmt.Errorf("capture evidence differs")
	}
	*output = true
	return nil
}

func TestInputReadMetadataReadyBeforeCaptureAndInit(t *testing.T) {
	result, err := New().Execute(context.Background(), Request{
		Input:   testRouteInput(t, reflect.TypeOf(metadataLifecycleInput{})),
		Handler: custom.New[metadataLifecycleInput, bool](&metadataLifecycleContract{}),
		Providers: []locator.Provider{provider.New("metadataFixture", func(context.Context) (any, bool, error) {
			return locator.ValueWithMetadata{Value: []int{1}, Metadata: &metadataTestProjection{}}, true, nil
		})},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !*result.(*bool) {
		t.Fatal("handler did not execute")
	}
}

func TestBoundInputBindsSupplementalReadsAndMetadata(t *testing.T) {
	input := &metadataLifecycleInput{}
	contract := testRouteInput(t, reflect.TypeOf(metadataLifecycleInput{}), bindly.BindingSpec{
		Path: "Rows", Location: bindstate.Location{Kind: "metadataFixture", In: "Rows"},
	})
	if fields := contract.Fields(); len(fields) != 1 || fields[0].Binding().Location.Kind != "metadataFixture" {
		t.Fatalf("fields=%#v", fields)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := boundSupplementalPlan(injector, contract)
	if err != nil || plan == nil {
		t.Fatalf("supplemental plan=%v err=%v", plan, err)
	}
	result, err := New().Execute(context.Background(), Request{
		Input:      contract,
		BoundInput: input,
		Handler:    custom.New[metadataLifecycleInput, bool](&metadataLifecycleContract{}),
		Providers: []locator.Provider{provider.Named("metadataFixture", func(context.Context, reflect.Type, string) (any, bool, error) {
			return locator.ValueWithMetadata{Value: []int{1}, Metadata: &metadataTestProjection{}}, true, nil
		})},
	})
	if err != nil {
		t.Fatalf("%v input=%#v", err, input)
	}
	if !*result.(*bool) || len(input.Rows) != 1 || input.Rows[0] != 1 {
		t.Fatalf("result=%v input=%#v", result, input)
	}
}

func TestOrdinaryHandlerShadowsParentReadMetadata(t *testing.T) {
	parent := &inputReadMetadata{ready: true, projections: map[string]xhandler.ReadProjection{"Rows": &metadataTestProjection{}}}
	ctx := xhandler.WithReadMetadata(context.Background(), parent)
	_, err := New().Execute(ctx, Request{
		Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{},
		Handler: rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			if _, ok := xhandler.ReadMetadataFromContext(ctx); ok {
				return nil, fmt.Errorf("parent metadata leaked")
			}
			if _, _, err := invocation.Binder.Lookup(ctx, xhandler.ReadMetadataKey); err == nil {
				return nil, fmt.Errorf("ordinary handler obtained read metadata")
			}
			return nil, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if actual, ok := xhandler.ReadMetadataFromContext(ctx); !ok || actual != parent {
		t.Fatal("parent context mutated")
	}
}

type metadataConsumerHandler struct{ rhandler.HandlerFunc }

func (metadataConsumerHandler) RequiresReadMetadata() bool { return true }

func TestNestedConsumersHaveIndependentReadMetadata(t *testing.T) {
	inputType := reflect.TypeOf(struct {
		Rows []int `bind:"kind=metadataFixture"`
	}{})
	input := testRouteInput(t, inputType)
	parentProjection := &metadataTestProjection{identity: 1}
	childProjection := &metadataTestProjection{identity: 2}
	providers := func(projection xhandler.ReadProjection) []locator.Provider {
		return []locator.Provider{provider.New("metadataFixture", func(context.Context) (any, bool, error) {
			return locator.ValueWithMetadata{Value: []int{1}, Metadata: projection}, true, nil
		})}
	}
	_, err := New().Execute(context.Background(), Request{Input: input, Providers: providers(parentProjection),
		Handler: metadataConsumerHandler{HandlerFunc: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
			parent, ok := xhandler.ReadMetadataFromContext(ctx)
			if !ok {
				return nil, fmt.Errorf("parent metadata missing")
			}
			_, err := New().Execute(ctx, Request{Input: input, Providers: providers(childProjection),
				Handler: metadataConsumerHandler{HandlerFunc: func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
					child, ok := xhandler.ReadMetadataFromContext(ctx)
					if !ok || child == parent {
						return nil, fmt.Errorf("child borrowed parent metadata")
					}
					actual, err := child.Projection("Rows")
					if err != nil {
						return nil, err
					}
					if actual != childProjection {
						return nil, fmt.Errorf("child projection crossed invocation")
					}
					return nil, nil
				}},
			})
			if err != nil {
				return nil, err
			}
			actual, err := parent.Projection("Rows")
			if err != nil {
				return nil, err
			}
			if actual != parentProjection {
				return nil, fmt.Errorf("child changed parent metadata")
			}
			return nil, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestInputReadMetadataAssignmentLifecycle(t *testing.T) {
	ctx := context.Background()
	input, nested := new(int), new(int)
	projection := &metadataTestProjection{}
	for _, test := range []struct {
		name        string
		replacement any
		want        bool
	}{
		{"known", projection, true}, {"opaque", "opaque", false}, {"missing", nil, false}, {"typed nil", (*metadataTestProjection)(nil), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata := &inputReadMetadata{target: input, projections: make(map[string]xhandler.ReadProjection)}
			if _, err := metadata.Projection("Current"); err == nil || !strings.Contains(err.Error(), "during binding") {
				t.Fatalf("premature evidence: %v", err)
			}
			for _, event := range []bindly.BindingEvent{
				{Target: input, Path: "Current", Metadata: projection},
				{Target: nested, Path: "Nested", Metadata: projection},
				{Target: input, Path: "Current", Metadata: test.replacement},
			} {
				if err := metadata.observe(ctx, event); err != nil {
					t.Fatal(err)
				}
			}
			metadata.seal()
			got, err := metadata.Projection("Current")
			if (err == nil) != test.want || (test.want && got != projection) {
				t.Fatalf("projection=%v error=%v", got, err)
			}
			if _, err := metadata.Projection("Nested"); err == nil {
				t.Fatal("nested target leaked into input metadata")
			}
			if err := metadata.observe(ctx, bindly.BindingEvent{Target: input, Path: "Current"}); err == nil {
				t.Fatal("sealed assignment accepted")
			}
		})
	}
}

func TestReadMetadataProviderCannotBeOverridden(t *testing.T) {
	fake := []locator.Provider{provider.Static(xhandler.ReadMetadataKey, "forged")}
	for _, input := range []providerComposition{{component: fake}, {protocol: fake}, {child: fake}} {
		if _, err := (providerComposer{}).compose(input); err == nil {
			t.Fatal("untrusted metadata provider accepted")
		}
	}
}
