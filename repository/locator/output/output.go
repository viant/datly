package output

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/exec"
	"github.com/viant/xdatly/handler/response"
)

type Locator struct {
	Output           *reader.Output
	Status           *response.Status
	OutputParameters state.NamedParameters
	Metrics          response.Metrics
}

func (l *Locator) Names() []string {
	return nil
}

func (l *Locator) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {
	aName := strings.ToLower(name)
	switch aName {
	case keys.ViewData:
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.Data, true, nil
	case keys.ViewSummaryData:
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.DataSummary, true, nil
	case keys.Status:
		if l.Status == nil {
			return nil, false, nil
		}
		return l.Status, true, nil
	case keys.Nil:
		return nil, false, nil
	case keys.Error:
		if err := ctx.Value(exec.ErrorKey); err != nil {
			e, _ := err.(error)
			return e.Error(), true, nil
		}
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
	case keys.Metrics:
		if l.Metrics == nil {
			return nil, true, nil
		}
		return l.Metrics, true, nil
	case keys.SQL:
		if l.Output == nil {
			return nil, false, nil
		}
		SQL := l.Output.Metrics.SQL()
		SQL = strings.ReplaceAll(SQL, "\n", "\t ")
		return SQL, true, nil
	case "zero":
		return 0, true, nil
	case "empty":
		return 0, true, nil
	default:
		switch {
		case strings.HasPrefix(aName, keys.Response):
			return l.getResponseValue(ctx, aName)
		case strings.HasPrefix(aName, keys.Filters):
			return l.getFiltersValue(ctx)
		case strings.HasPrefix(aName, keys.Filter):
			return l.getFilterValue(ctx)
		}
	}
	return nil, false, nil
}

// newOutputLocator returns output locator
func newOutputLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	ret := &Locator{OutputParameters: options.OutputParameters}
	for _, candidate := range options.Custom {
		if output, ok := candidate.(*reader.Output); ok {
			ret.Output = output
		}
		if status, ok := candidate.(*response.Status); ok {
			ret.Status = status
		}
	}
	ret.Metrics = options.Metrics
	return ret, nil
}

func init() {
	locator.Register(state.KindOutput, newOutputLocator)
}
