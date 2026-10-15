package scopedsourced

import (
	"context"
	"fmt"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/xdatly"
	h "github.com/viant/xdatly/handler"
	modernresponse "github.com/viant/xdatly/response"
)

// Component uses the same linked factory/connector metadata as an emitted custom holder.
type Component struct {
	NoSource xdatly.Component[ParentInput, Output] `component:"NoSource,path=/scoped-nosource,method=POST,handler=NewNoSource"`
	Parent   xdatly.Component[ParentInput, Output] `component:"Parent,path=/scoped-parent,method=POST,connector=main,handler=NewParent"`
	Run      xdatly.Component[Input, Output]       `component:"Run,path=/scoped-source,method=POST,connector=main,handler=NewRun"`
}

var LinkedType = reflect.TypeFor[Component]()

func (Component) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewNoSource" {
		return custom.Factory(NewNoSource)
	}
	if name == "NewParent" {
		return custom.Factory(NewParent)
	}
	if name == "NewRun" {
		return custom.Factory(NewRun)
	}
	return nil
}

type Input struct {
	Selection  int                                                               `parameter:"Selection,kind=query,in=selection,required=true"`
	Current    []*ViewRow                                                        `parameter:"Current,kind=view,in=Current" view:"Current,table=scoped_current" sql:"SELECT id,name FROM scoped_current WHERE id=:Selection"`
	Later      []*ViewRow                                                        `parameter:"Later,kind=view,in=Later,scope=lazy" view:"Later,table=scoped_later" sql:"SELECT id,name,(SELECT COUNT(*) FROM scoped_audit) AS queued FROM scoped_later WHERE id=(:Selection+11)"`
	Credential *string                                                           `parameter:",kind=header,in=Authorization,required=true,errorCode=401"`
	Mode       string                                                            `parameter:",kind=query,in=mode"`
	Lazy       *string                                                           `parameter:",kind=header,in=X-Lazy,scope=lazy,required=true"`
	Has        *struct{ Credential, Mode, Lazy, Current, Later, Selection bool } `setMarker:"true"`
}
type ViewRow struct {
	ID     int    `sqlx:"id"`
	Name   string `sqlx:"name"`
	Queued int    `sqlx:"queued"`
}
type Output struct {
	Stage string `json:"stage"`
}
type Row struct {
	ID   int    `sqlx:"id,primaryKey"`
	Name string `sqlx:"name" validate:"required"`
}
type fields map[string]bool

func (f fields) Has(name string) bool { return f[name] }

type run struct{}

func NewRun() h.Contract[Input, Output] { return &run{} }
func (*run) BindsInputByScope() bool    { return true }
func (*run) Exec(ctx context.Context, s h.Session, i *Input, o *Output) error {
	if i.Credential == nil || i.Has == nil || !i.Has.Credential || i.Lazy != nil || i.Has.Lazy {
		return fmt.Errorf("default/lazy binding order changed")
	}
	if len(i.Current) != 1 || i.Current[0].ID != 11 || i.Current[0].Name != "current" || !i.Has.Current || i.Later != nil || i.Has.Later {
		return fmt.Errorf("default SQL view changed")
	}
	if i.Selection != 11 || !i.Has.Selection {
		return fmt.Errorf("canonical SQL parameter missing")
	}
	marker := i.Has
	b := s.Binder().(h.ScopedBinder)
	canonical, found, err := b.Lookup(ctx, h.InputKey)
	if err != nil || !found || canonical != i {
		return fmt.Errorf("canonical input changed")
	}

	if *i.Credential == "deny" {
		return &modernresponse.Error{Code: 403, Cause: fmt.Errorf("denied before lazy")}
	}
	if i.Mode == "skip-lazy" {
		o.Stage = "default,handler"
		return nil
	}
	d, found, err := b.Lookup(ctx, h.DMLKey)
	if err != nil || !found {
		return fmt.Errorf("DML missing: %v", err)
	}
	v, found, err := b.Lookup(ctx, h.FrameworkValidatorKey)
	if err != nil || !found {
		return fmt.Errorf("protected validation missing: %v", err)
	}
	row := &Row{ID: 1, Name: "valid"}
	if i.Mode == "validation" {
		row.Name = ""
	}
	off := false
	validation, err := v.(h.Validator).Validate(ctx, []*Row{row}, []h.ValidationOptions{{CheckOnly: true, Shallow: true, Fields: fields{"Name": true}, GoFields: fields{"Name": true}, CheckUnique: &off, CheckRef: &off}})
	if err != nil {
		return err
	}
	if err := validation.Err(); err != nil {
		return err
	}
	if err := d.(h.DML).Insert("scoped_audit", row); err != nil {
		return err
	}
	if err := b.BindScope(ctx, i, "lazy"); err != nil {
		return err
	}
	if i.Has != marker || !i.Has.Lazy || i.Lazy == nil || *i.Lazy != "later" {
		return fmt.Errorf("lazy binding missing")
	}
	if len(i.Later) != 1 || i.Later[0].ID != 22 || i.Later[0].Name != "later" || i.Later[0].Queued != 0 || !i.Has.Later || i.Has != marker {
		return fmt.Errorf("lazy SQL view or pending buffer changed")
	}
	if i.Mode == "handler" {
		return fmt.Errorf("handler rejected")
	}
	if i.Mode == "late" {
		if err := d.(h.DML).Insert("missing_scoped_audit", &Row{ID: 2, Name: "later"}); err != nil {
			return err
		}
	}
	o.Stage = "default,handler,validated,queued,lazy"
	return nil
}

type ParentInput struct {
	Mode string `parameter:",kind=query,in=mode"`
}
type parent struct{}

func NewParent() h.Contract[ParentInput, Output] { return &parent{} }
func (*parent) Exec(ctx context.Context, s h.Session, i *ParentInput, o *Output) error {
	b := s.Binder()
	d, found, err := b.Lookup(ctx, h.DMLKey)
	if err != nil || !found {
		return fmt.Errorf("parent DML missing: %v", err)
	}
	if err := d.(h.DML).Insert("scoped_audit", &Row{ID: 90, Name: "caller"}); err != nil {
		return err
	}
	value, found, err := b.Lookup(ctx, h.ComponentInvokerKey)
	if err != nil || !found {
		return fmt.Errorf("invoker missing: %v", err)
	}
	child, err := value.(h.ComponentInvoker).Invoke(ctx, h.ComponentRequest{Target: h.ComponentTarget{Scope: "github.com/viant/datly/standalone/testdata/scopedsourced", Name: "Run", Method: "POST", Path: "/scoped-source"}})
	if err != nil {
		return err
	}
	if i.Mode == "caller" {
		return fmt.Errorf("caller rejected")
	}
	*o = *(child.(*Output))
	return nil
}

type noSource struct{}

func NewNoSource() h.Contract[ParentInput, Output] { return &noSource{} }
func (*noSource) Exec(ctx context.Context, s h.Session, i *ParentInput, o *Output) error {
	value, found, err := s.Binder().Lookup(ctx, h.ComponentInvokerKey)
	if err != nil || !found {
		return fmt.Errorf("invoker missing: %v", err)
	}
	child, err := value.(h.ComponentInvoker).Invoke(ctx, h.ComponentRequest{Target: h.ComponentTarget{Scope: "github.com/viant/datly/standalone/testdata/scopedsourced", Name: "Run", Method: "POST", Path: "/scoped-source"}})
	if err != nil {
		return err
	}
	if i.Mode == "caller" {
		return fmt.Errorf("source-less caller rejected")
	}
	*o = *(child.(*Output))
	return nil
}
