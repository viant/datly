package namedfactory

import (
	"context"
	"fmt"
	xdatly "github.com/viant/xdatly"
	"strings"

	h "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/viant/xdatly/handler/mutation"
)

type Component struct {
	Regular  xdatly.Component[Input, Output] `component:"Regular,path=/regular,method=POST,handler=NewRegular"`
	Mutation xdatly.Component[Input, Output] `component:"Mutation,path=/mutation,method=POST,handler=NewMutation"`
}

type Record struct {
	ID   int    `sqlx:"id,primaryKey=true" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
type Input struct {
	Record *Record  `parameter:"Record,kind=body,in=data,required"`
	Events []string `json:"-"`
}

func (i *Input) Init(context.Context) error { i.Events = append(i.Events, "input init"); return nil }
func (i *Input) InitMCP(context.Context, xmcp.Context) error {
	i.Events = append(i.Events, "input MCP")
	return nil
}

type Output struct {
	Data   *Record  `json:"data"`
	Events []string `json:"events"`
}

func (o *Output) Finalize(context.Context) error {
	o.Events = append(o.Events, "regular finalize")
	return nil
}

type regular struct {
	wait    <-chan struct{}
	started chan<- struct{}
}

func NewRegular() h.Contract[Input, Output] { return &regular{} }
func NewBlockingRegular(wait <-chan struct{}, started chan<- struct{}) h.Contract[Input, Output] {
	return &regular{wait: wait, started: started}
}
func (r *regular) Exec(ctx context.Context, session h.Session, input *Input, output *Output) error {
	if r.started != nil {
		r.started <- struct{}{}
	}
	if r.wait != nil {
		select {
		case <-r.wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	value, found, err := session.Binder().Lookup(ctx, h.DMLKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("DML unavailable")
	}
	input.Record.Name += ":regular"
	if err := value.(h.DML).Insert("records", input.Record); err != nil {
		return err
	}
	output.Data = input.Record
	output.Events = append(input.Events, "regular execute")
	return nil
}

type definition struct{}

func NewMutation() mutation.Definition[Input, Output] { return &definition{} }
func (*definition) Capture(_ context.Context, input *Input) (mutation.Program[Output], error) {
	if len(input.Events) != 0 {
		return nil, fmt.Errorf("capture ran after input initialization")
	}
	return &program{input: input, output: &Output{Data: input.Record}}, nil
}
func (*definition) FinalizeFailure(context.Context, *Input, *Output, h.Outcome) error { return nil }

type program struct {
	input  *Input
	output *Output
	dml    h.DML
}

func (p *program) Prepare(ctx context.Context, binder h.Binder) error {
	p.input.Events = append([]string{"capture"}, p.input.Events...)
	value, found, err := binder.Lookup(ctx, h.DMLKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("DML unavailable")
	}
	p.dml = value.(h.DML)
	p.input.Events = append(p.input.Events, "prepare")
	return nil
}
func (p *program) SyncPresence(context.Context) error {
	p.input.Events = append(p.input.Events, "sync")
	return nil
}
func (p *program) Invariants(context.Context) error {
	p.input.Events = append(p.input.Events, "invariants")
	return nil
}
func (p *program) Init(context.Context) error {
	p.input.Record.Name += ":mutation"
	p.input.Events = append(p.input.Events, "entity init")
	return nil
}
func (p *program) Validate(context.Context) error {
	p.input.Events = append(p.input.Events, "validate")
	if strings.HasPrefix(p.input.Record.Name, "reject") {
		return fmt.Errorf("entity rejected")
	}
	return nil
}
func (*program) RequiresTransaction() bool { return true }
func (p *program) Sequence(context.Context) error {
	p.input.Events = append(p.input.Events, "sequence")
	return nil
}
func (p *program) Diff(context.Context) error {
	p.input.Events = append(p.input.Events, "diff")
	return nil
}
func (p *program) Reconcile(context.Context) error {
	p.input.Events = append(p.input.Events, "reconcile")
	return nil
}
func (p *program) Queue(context.Context) error {
	p.input.Events = append(p.input.Events, "queue")
	return p.dml.Insert("records", p.input.Record)
}
func (p *program) Output() *Output { p.output.Events = p.input.Events; return p.output }
func (p *program) Finalize(_ context.Context, outcome h.Outcome) error {
	p.input.Events = append(p.input.Events, "finalize "+string(outcome.State()))
	p.output.Events = p.input.Events
	return nil
}
