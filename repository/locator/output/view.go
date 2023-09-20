package output

import (
	"context"
	"github.com/viant/datly/repository/locator/output/keys"
	"strings"
)

func (l *outputLocator) getViewValue(ctx context.Context, name string) (interface{}, bool, error) {
	switch name {
	case keys.ViewID:
		return strings.ToUpper(l.View.Name), true, nil
	case keys.ViewName:
		return l.View.Name, true, nil
	case keys.ViewDescription:
		return l.View.Description, true, nil
	}
	return nil, false, nil
}
