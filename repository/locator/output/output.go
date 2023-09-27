package output

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/response"
	"strings"
)

type outputLocator struct {
	Output           *reader.Output
	Status           *response.Status
	OutputParameters state.NamedParameters
	View             *view.View
	Metrics          reader.Metrics
}

func (l *outputLocator) Names() []string {
	return nil
}

func (l *outputLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	aName := strings.ToLower(name)
	switch aName {
	case keys.Data:
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.Data, true, nil
	case keys.Summary, keys.SummaryMeta:
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.ViewMeta, true, nil
	case keys.Status:
		if l.Status == nil {
			return nil, false, nil
		}
		return l.Status, true, nil
	case "empty":
		return "", true, nil
	case "zero":
		return 0, true, nil
	case keys.Error:
		if l.Status == nil || l.Status.Status == "ok" {
			return "", true, nil
		}
		if l.Status.Message != "" {
			return l.Status.Message, true, nil
		}
		if l.Status.Errors != nil {
			message, _ := json.Marshal(l.Status.Errors)
			return string(message), true, nil
		}
		return "", false, nil
	case keys.StatusCode:
		if l.Status == nil {
			return "unknown", true, nil
		}
		return l.Status.Status, true, nil
	case keys.SQL:
		if l.Output == nil {
			return nil, false, nil
		}
		SQL := l.Output.Metrics.SQL()
		return SQL, true, nil
	default:
		switch {
		case strings.HasPrefix(aName, keys.Job):
			return l.getJobValue(ctx, aName)
		case strings.HasPrefix(aName, "async"):
			return l.getAsyncValue(ctx, aName)
		case strings.HasPrefix(aName, "response"):
			return l.getResponseValue(ctx, aName)
		case strings.HasPrefix(aName, keys.Filter):
			return l.getFilterValue(ctx)
		case strings.HasPrefix(aName, keys.Filters):
			return l.getFiltersValue(ctx)
		case strings.HasPrefix(aName, "view"):
			return l.getViewValue(ctx, aName)
		}
	}
	return nil, false, nil
}

// newOutputLocator returns output locator
func newOutputLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	ret := &outputLocator{OutputParameters: options.OutputParameters}
	for _, candidate := range options.Custom {
		if output, ok := candidate.(*reader.Output); ok {
			ret.Output = output
		}
		if status, ok := candidate.(*response.Status); ok {
			ret.Status = status
		}
	}
	ret.View = options.View
	ret.Metrics = options.Metrics
	return ret, nil
}

func init() {
	locator.Register(state.KindOutput, newOutputLocator)
}
