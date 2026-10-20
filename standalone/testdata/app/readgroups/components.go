package readgroups

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/xdatly"
)

type Components struct {
	Subject   xdatly.Component[Input, Output]           `component:"Subject,path=/read-group,method=GET,connector=main" mcp:"[{\"kind\":\"tool\",\"name\":\"ReadGroup\"}]"`
	Left      xdatly.Component[LeftInput, LeftOutput]   `component:"ReadLeft,path=/read-group/left,method=GET,connector=main,internal=true"`
	Right     xdatly.Component[RightInput, RightOutput] `component:"ReadRight,path=/read-group/right,method=POST,connector=main,internal=true"`
	Leaf      xdatly.Component[LeafInput, LeafOutput]   `component:"ReadLeaf,path=/read-group/leaf,method=GET,connector=main,internal=true"`
	ExtraLeaf xdatly.Component[LeafInput, LeafOutput]   `component:"ReadExtraLeaf,path=/read-group/leaf-extra,method=GET,connector=main,internal=true"`
}

var Datly = new(Components)
var DatlyLinkedType = reflect.TypeFor[Components]()

type Input struct {
	Case  string       `parameter:",kind=query,in=case" json:"case"`
	Left  *LeftOutput  `parameter:",kind=component,in=GET:/read-group/left,resolutionGroup=subject,resolutionMode=dependencies"`
	Right *RightOutput `parameter:",kind=component,in=POST:/read-group/right,resolutionGroup=subject,resolutionMode=dependencies"`
}

type LeftInput struct {
	Case       string      `parameter:",kind=query,in=case"`
	Leaf       *LeafOutput `parameter:",kind=component,in=GET:/read-group/leaf,resolutionGroup=left,resolutionMode=dependencies"`
	SecondLeaf *LeafOutput `parameter:",kind=component,in=GET:/read-group/leaf-extra,resolutionGroup=left,resolutionMode=dependencies"`
}
type RightInput struct {
	Case string      `parameter:",kind=query,in=case"`
	Leaf *LeafOutput `parameter:",kind=component,in=GET:/read-group/leaf"`
}
type LeafInput struct {
	Case string `parameter:",kind=query,in=case"`
}

type Row struct {
	ID   int    `sqlx:"id" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
type Output struct {
	Data  []*Row `parameter:",kind=output,in=view" view:"subject" sql:"SELECT id,name FROM records" json:"data"`
	Left  string `json:"left"`
	Right string `json:"right"`
}
type LeftOutput struct {
	Data     []*Row `parameter:",kind=output,in=view" view:"left" sql:"SELECT id,name FROM records" json:"data"`
	Complete bool   `json:"complete"`
}
type RightOutput struct {
	Data     []*Row `parameter:",kind=output,in=view" view:"right" sql:"SELECT id,name FROM records" json:"data"`
	Complete bool   `json:"complete"`
}
type LeafOutput struct {
	Data     []*Row `parameter:",kind=output,in=view" view:"leaf" sql:"SELECT id,name FROM records" json:"data"`
	Complete bool   `json:"complete"`
}

// Each invocation selects its own probe. No mutable hook state is shared by
// requests; tests can coordinate real native readers without timing assumptions.
type Probe struct {
	Entered chan string
	Release <-chan struct{}
	Failure error
}

var probes sync.Map

func Install(name string, probe *Probe) func() {
	probes.Store(name, probe)
	return func() { probes.Delete(name) }
}
func wait(ctx context.Context, name, branch string) error {
	value, found := probes.Load(name)
	if !found {
		return nil
	}
	probe := value.(*Probe)
	select {
	case probe.Entered <- branch:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-probe.Release:
		return probe.Failure
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *LeafOutput) Finalize(_ context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	o.Complete = true
	return nil
}
func (o *LeftOutput) Finalize(ctx context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	i := ctx.Value(reflect.TypeFor[*LeftInput]()).(*LeftInput)
	if i.Leaf == nil || !i.Leaf.Complete || i.SecondLeaf == nil || !i.SecondLeaf.Complete {
		return errors.New("left observed incomplete nested output")
	}
	if err := wait(ctx, i.Case, "left"); err != nil {
		return err
	}
	o.Complete = true
	return nil
}
func (o *RightOutput) Finalize(ctx context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	i := ctx.Value(reflect.TypeFor[*RightInput]()).(*RightInput)
	if i.Leaf == nil || !i.Leaf.Complete {
		return errors.New("right observed incomplete nested output")
	}
	if err := wait(ctx, i.Case, "right"); err != nil {
		return err
	}
	o.Complete = true
	return nil
}
func (o *Output) Finalize(ctx context.Context, cause error) error {
	if cause != nil {
		return cause
	}
	i := ctx.Value(reflect.TypeFor[*Input]()).(*Input)
	if i.Left == nil || i.Right == nil || !i.Left.Complete || !i.Right.Complete {
		return errors.New("parent observed incomplete dependencies")
	}
	o.Left = fmt.Sprintf("left:%s:%d", i.Case, len(i.Left.Data))
	o.Right = fmt.Sprintf("right:%s:%d", i.Case, len(i.Right.Data))
	return nil
}
