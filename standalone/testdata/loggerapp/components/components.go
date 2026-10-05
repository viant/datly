package components

import (
	"context"
	"embed"
	"fmt"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/spec"
	"github.com/viant/xdatly"
	xdiffer "github.com/viant/xdatly/differ"
	h "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/logger"
	"reflect"
)

//go:embed queries/*.sql
var Queries embed.FS

const Package = "github.com/viant/datly/standalone/testdata/loggerapp/components"

type Component struct {
	Patch  xdatly.Component[PatchInput, PatchOutput]   `component:"Patch,path=/differ,method=PATCH,connector=main,view=Records" mutation:"patch"`
	Read   xdatly.Component[ReadInput, ReadOutput]     `component:"Read,path=/logged/{id},method=GET,connector=main,view=Records"`
	Parent xdatly.Component[ParentInput, ParentOutput] `component:"Parent,path=/parent/{id},method=GET,handler=NewParent"`
	Write  xdatly.Component[WriteInput, WriteOutput]   `component:"Write,path=/logged,method=POST,connector=main,view=Records" mutation:"post"`
}

func (Component) EmbedFS() *embed.FS { return &Queries }
func (Component) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewParent" || name == Package+".NewParent" {
		return custom.Factory(NewParent)
	}
	return nil
}

var _datlyReachableComponent = reflect.TypeFor[Component]()

type Record struct {
	ID   int        `sqlx:"id,primaryKey=true" json:"id"`
	Name string     `sqlx:"name" json:"name"`
	Has  *RecordHas `setMarker:"true" json:"-" sqlx:"-"`
}
type RecordHas struct{ ID, Name bool }
type ReadInput struct {
	ID     int           `parameter:"ID,kind=path,in=id,required"`
	Logger logger.Logger `parameter:"Logger,kind=logger,required=true" json:"-"`
}

func (i *ReadInput) Init(context.Context) error {
	if i.Logger == nil {
		return fmt.Errorf("reader input logger missing")
	}
	i.Logger.Debug("reader.input", "id", i.ID)
	return nil
}

type ReadOutput struct {
	Rows   []*Record     `parameter:"Rows,kind=output,in=view" view:"Records,table=records" sql:"uri=queries/read.sql" json:"rows"`
	Logger logger.Logger `parameter:"Logger,kind=logger,required=true" json:"-"`
}

func (o *ReadOutput) Finalize(context.Context) error {
	if o.Logger == nil {
		return fmt.Errorf("reader output logger missing")
	}
	o.Logger.Warn("reader.output", "rows", len(o.Rows))
	return nil
}

type ParentInput struct {
	ID      int                    `parameter:"ID,kind=path,in=id,required"`
	Logger  logger.Logger          `parameter:"Logger,kind=logger,required=true" json:"-"`
	Invoker dexec.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required=true" json:"-"`
}
type ParentOutput struct {
	Rows []*Record `json:"rows"`
}
type parent struct{}

func NewParent() h.Contract[ParentInput, ParentOutput] { return &parent{} }
func (*parent) Exec(ctx context.Context, _ h.Session, in *ParentInput, out *ParentOutput) error {
	if in.Logger == nil || in.Invoker == nil {
		return fmt.Errorf("parent capabilities missing")
	}
	in.Logger.Info("parent.begin", "id", in.ID)
	value, err := in.Invoker.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: Package, Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/logged/{id}"}}})
	if err != nil {
		return err
	}
	child, ok := value.(*ReadOutput)
	if !ok {
		return fmt.Errorf("unexpected child %T", value)
	}
	out.Rows = child.Rows
	in.Logger.Info("parent.end", "rows", len(out.Rows))
	return nil
}

type WriteInput struct {
	Rows []*Record `parameter:"Rows,kind=body,in=data,required=true" view:"Records,table=records,entityHooks=RecordLifecycle" sql:"uri=queries/write.sql"`
}
type WriteOutput struct {
	Rows []*Record `parameter:"Rows,kind=output,in=body" json:"rows"`
}
type RecordLifecycle struct {
	Logger h.Logger `bind:"kind=logger,required"`
}

func RecordLifecycleDatlyType() reflect.Type { return reflect.TypeFor[RecordLifecycle]() }

var RecordLifecycleDatly = RecordLifecycleDatlyType()

func (hook *RecordLifecycle) Init(_ context.Context, row *Record, _ h.LifecycleContext[Record, h.NoParent, WriteOutput]) error {
	if hook.Logger == nil {
		return fmt.Errorf("writer lifecycle logger missing")
	}
	hook.Logger.Info("writer.init", "id", row.ID)
	return nil
}
func (hook *RecordLifecycle) Finalize(_ context.Context, _ *WriteInput, _ *WriteOutput, outcome h.Outcome) error {
	if outcome.Error != nil {
		return outcome.Error
	}
	hook.Logger.Info("writer.finalize", "transaction", outcome.State())
	return nil
}

// Patch exercises trusted Differ binding on the same metadata-driven writer
// used by generated mutation components.
type PatchInput struct {
	Rows    []*Record `parameter:"Rows,kind=body,in=data,required=true" view:"Records,table=records,entityHooks=DifferLifecycle" sql:"uri=queries/write.sql"`
	Current []*Record `parameter:"Current,kind=view,in=Current" view:"Current,table=records" sql:"uri=queries/write.sql"`
}
type PatchOutput struct {
	Rows          []*Record               `parameter:"Rows,kind=output,in=body" json:"rows"`
	Changes       []*xdiffer.ChangeRecord `json:"changes"`
	PreviousNames []string                `json:"previousNames"`
	BoundDiffer   xdiffer.Differ          `json:"-"`
}
type DifferLifecycle struct {
	Differ xdiffer.Differ `bind:"kind=differ,required"`
}

func DifferLifecycleDatlyType() reflect.Type { return reflect.TypeFor[DifferLifecycle]() }

var DifferLifecycleDatly = DifferLifecycleDatlyType()

func (hook *DifferLifecycle) Init(ctx context.Context, row *Record, state h.LifecycleContext[Record, h.NoParent, PatchOutput]) error {
	state.Output.BoundDiffer = hook.Differ
	if state.Previous != nil {
		state.Output.PreviousNames = append(state.Output.PreviousNames, state.Previous.Name)
	}
	changes, err := hook.Differ.Diff(ctx, state.Previous, row, xdiffer.WithShallow(true), xdiffer.WithSetMarker(true))
	if err != nil {
		return err
	}
	state.Output.Changes = append(state.Output.Changes, changes.ToChangeRecords()...)
	return nil
}

func (ReadInput) EmbedFS() *embed.FS  { return &Queries }
func (WriteInput) EmbedFS() *embed.FS { return &Queries }
func (PatchInput) EmbedFS() *embed.FS { return &Queries }
