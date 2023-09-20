package output

import "context"

func (l *outputLocator) getViewValue(ctx context.Context, name string) (interface{}, bool, error) {
	switch name {
	case "view.name":
		return l.View.Name, true, nil
	case "view.description":
		return l.View.Description, true, nil
	}
	return nil, false, nil
}
