package seedgroups

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/testdata/app/readgroups"
	"github.com/viant/xdatly"
	xcodec "github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler"
)

type Components struct {
	Producer   xdatly.Component[ProducerInput, ProducerOutput] `component:"SeedProducer,path=/seed-producer,method=GET,handler=NewProducer"`
	Reader     xdatly.Component[Input, Output]                 `component:"SeedReader,path=/seed-reader,method=GET,connector=main,internal=true"`
	Sequential xdatly.Component[SequentialInput, Output]       `component:"SequentialReader,path=/seed-sequential,method=GET,connector=main,internal=true"`
}

var Datly = new(Components)
var DatlyLinkedType = reflect.TypeFor[Components]()
var LoaderLinkedType = reflect.TypeFor[SnapshotLoader]()

func (Components) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewProducer" {
		return custom.Factory(NewProducer)
	}
	return nil
}

type Input struct {
	AutomationURL       string                  `parameter:",kind=const,in=automationURL,value=/missing-default-snapshot"`
	PrevOrderAutomation []SnapshotRow           `parameter:",kind=param,in=AutomationURL,dataType=string" codec:"github.com/viant/datly/standalone/testdata/app/seedgroups.SnapshotLoader"`
	Case                string                  `parameter:",kind=query,in=case"`
	A                   *readgroups.LeftOutput  `parameter:",kind=component,in=GET:/read-group/left,resolutionGroup=automation,resolutionMode=dependencies"`
	B                   *readgroups.RightOutput `parameter:",kind=component,in=POST:/read-group/right,resolutionGroup=automation,resolutionMode=dependencies"`
	Has                 *Presence               `setMarker:"true"`
}

type SequentialInput struct {
	AutomationURL       string                  `parameter:",kind=const,in=automationURL,value=/missing-default-snapshot"`
	PrevOrderAutomation []SnapshotRow           `parameter:",kind=param,in=AutomationURL,dataType=string" codec:"github.com/viant/datly/standalone/testdata/app/seedgroups.SnapshotLoader"`
	Case                string                  `parameter:",kind=query,in=case"`
	A                   *readgroups.LeftOutput  `parameter:",kind=component,in=GET:/read-group/left"`
	B                   *readgroups.RightOutput `parameter:",kind=component,in=POST:/read-group/right"`
	Has                 *Presence               `setMarker:"true"`
}
type Presence struct{ AutomationURL, PrevOrderAutomation, Case, A, B bool }
type SnapshotRow struct {
	ID   int `json:"id"`
	Mask int `json:"mask"`
}
type Output struct {
	Data     []*readgroups.Row `parameter:",kind=output,in=view" view:"seed" sql:"SELECT id,name FROM records" json:"data"`
	URL      string            `json:"url"`
	Snapshot []SnapshotRow     `json:"snapshot"`
	Lookups  map[string]int    `json:"lookups"`
}

func (o *Output) Finalize(ctx context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	var url string
	var snapshot []SnapshotRow
	var a *readgroups.LeftOutput
	var b *readgroups.RightOutput
	if i, ok := ctx.Value(reflect.TypeFor[*Input]()).(*Input); ok {
		url, snapshot, a, b = i.AutomationURL, i.PrevOrderAutomation, i.A, i.B
	} else if i, ok := ctx.Value(reflect.TypeFor[*SequentialInput]()).(*SequentialInput); ok {
		url, snapshot, a, b = i.AutomationURL, i.PrevOrderAutomation, i.A, i.B
	} else {
		return fmt.Errorf("reader input unavailable")
	}
	if a == nil || b == nil || !a.Complete || !b.Complete {
		return fmt.Errorf("parent observed incomplete dependencies")
	}
	o.URL, o.Snapshot = url, snapshot
	o.Lookups = map[string]int{"a": len(a.Data), "b": len(b.Data)}
	return nil
}

type SnapshotLoader struct{}

func (*SnapshotLoader) ResultType(reflect.Type) (reflect.Type, error) {
	return reflect.TypeFor[[]SnapshotRow](), nil
}
func (*SnapshotLoader) Value(ctx context.Context, raw any, _ ...xcodec.Option) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(raw.(string))
	if err != nil {
		return nil, err
	}
	var rows []SnapshotRow
	if err = json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, ctx.Err()
}

type ProducerInput struct {
	URL        string `parameter:",kind=query,in=url,required=true"`
	Case       string `parameter:",kind=query,in=case"`
	Sequential bool   `parameter:",kind=query,in=sequential"`
}
type producer struct{}
type ProducerOutput Output

func NewProducer() handler.Contract[ProducerInput, ProducerOutput] { return &producer{} }
func (*producer) Exec(ctx context.Context, s handler.Session, i *ProducerInput, o *ProducerOutput) error {
	value, found, err := s.Binder().Lookup(ctx, dexec.ComponentInvokerKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("component invoker unavailable")
	}
	name, path := "SeedReader", "/seed-reader"
	stale := []SnapshotRow{{ID: -1, Mask: -1}}
	seed := dexec.WithInput(&Input{AutomationURL: i.URL, PrevOrderAutomation: stale, Has: &Presence{AutomationURL: true}})
	if i.Sequential {
		name, path = "SequentialReader", "/seed-sequential"
		seed = dexec.WithInput(&SequentialInput{AutomationURL: i.URL, PrevOrderAutomation: stale, Has: &Presence{AutomationURL: true}})
	}
	result, err := value.(dexec.ComponentInvoker).InvokeComponent(ctx, dexec.ComponentRequest{
		Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: reflect.TypeFor[Components]().PkgPath(), Name: name}, Route: spec.RouteRef{Method: "GET", Path: path}}, ExtraInput: seed,
		Providers: []locator.Provider{provider.Named("query", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
			if name == "case" {
				return i.Case, true, nil
			}
			return nil, false, nil
		})},
	})
	if err != nil {
		return err
	}
	var seedURL string
	var previous []SnapshotRow
	var marks *Presence
	switch original := seed.Value().(type) {
	case *Input:
		seedURL, previous, marks = original.AutomationURL, original.PrevOrderAutomation, original.Has
	case *SequentialInput:
		seedURL, previous, marks = original.AutomationURL, original.PrevOrderAutomation, original.Has
	}
	if seedURL != i.URL || !reflect.DeepEqual(previous, []SnapshotRow{{ID: -1, Mask: -1}}) || !reflect.DeepEqual(marks, &Presence{AutomationURL: true}) {
		return fmt.Errorf("reader mutated caller-owned seed")
	}
	*o = ProducerOutput(*result.(*Output))
	return nil
}
