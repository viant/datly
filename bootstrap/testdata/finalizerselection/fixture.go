package finalizerselection

import (
	"context"
	"fmt"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
)

type Input struct{}
type Output struct {
	Data     []*Parent `parameter:"Data,kind=output,in=view" json:"data" view:"parents,type=Parent" sql:"SELECT id,name FROM parents"`
	Selected *bool     `json:"selected"`
}
type Parent struct {
	ID       int      `sqlx:"id" json:"id"`
	Name     string   `sqlx:"name" json:"name"`
	Children []*Child `json:"entries" view:"children,type=Child" sql:"SELECT id,parent_id FROM children" on:"ID:id=ParentID:parent_id"`
}
type Child struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=parents,property=fields"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"FinalizerSelection,path=/finalizer-selection,method=GET"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata only") }
func (*Output) RequiresOutputSelection() bool    { return true }
func (o *Output) Finalize(ctx context.Context, invoker handler.ComponentInvoker) error {
	provider, ok := invoker.(handler.OutputSelectionProvider)
	if !ok {
		return fmt.Errorf("output selection provider missing")
	}
	filter, available, err := provider.OutputSelection(ctx)
	if err != nil {
		return err
	}
	if !available {
		return fmt.Errorf("output selection unavailable")
	}
	selected := filter == nil || !filter.ExcludePath([]string{"data"}, "Children")
	o.Selected = &selected
	return nil
}

var Linked = new(Component)
