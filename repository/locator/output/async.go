package output

import (
	"context"
	"github.com/viant/datly/service/dispatcher/exec"
	"github.com/viant/xdatly/handler/async"
)

func (l *outputLocator) getAsyncValue(ctx context.Context, name string) (interface{}, bool, error) {
	infoValue := ctx.Value(exec.InfoKey)
	if infoValue == nil {
		return nil, false, nil
	}
	info := infoValue.(*exec.Info)
	switch name {
	case "async":
		return info.AsyncStatus(), true, nil
	case "async.done":
		return info.AsyncStatus() == string(async.StatusDone), true, nil
	case "async.elapsedInSec":
		return int(info.AsyncElapsed().Seconds()), true, nil
	case "async.elapsedInMs":
		return int(info.AsyncElapsed().Microseconds()), true, nil
	}
	return nil, false, nil
}
