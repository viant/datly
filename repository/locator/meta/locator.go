package meta

import (
	"context"
	"github.com/viant/datly/repository/locator/meta/keys"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"strings"
)

type metaLocator struct {
}

func (l *metaLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	value := ctx.Value(view.ContextKey)
	if value == nil {
		return nil, false, nil
	}
	aView := value.(*view.View)
	if aView == nil {
		return nil, false, nil
	}
	switch name {
	case keys.ViewID:
		return strings.ToUpper(aView.Name), true, nil
	case keys.ViewName:
		return aView.Name, true, nil
	case keys.ViewDescription:
		return aView.Description, true, nil
	}
	return nil, false, nil
}

func (l *metaLocator) Names() []string {
	return nil
}

func newMetaLocator(opts ...locator.Option) (kind.Locator, error) {
	ret := &metaLocator{}
	return ret, nil
}

func init() {
	locator.Register(state.KindMeta, newMetaLocator)
}
