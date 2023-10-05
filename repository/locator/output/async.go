package output

import (
	"context"
	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/service/processor/exec"
	"github.com/viant/xdatly/handler/async"
)

func (l *outputLocator) getAsyncValue(ctx context.Context, name string) (interface{}, bool, error) {
	infoValue := ctx.Value(exec.InfoKey)
	if infoValue == nil {
		return nil, false, nil
	}
	info := infoValue.(*exec.Info)
	switch name {
	case keys.AsyncStatus:
		return info.AsyncStatus(), true, nil
	case "async.done":
		return info.AsyncStatus() == string(async.StatusDone), true, nil
	case keys.AsyncElapsedInSec:
		return int(info.AsyncElapsed().Seconds()), true, nil
	case keys.AsyncElapsedInMs:
		return int(info.AsyncElapsed().Milliseconds()), true, nil

	case keys.AsyncCreationTime:
		v := info.AsyncCreationTime()
		return v, v != nil, nil
	case keys.AsyncCreationUnixTimeInSec:
		v := info.AsyncCreationTime()
		if v == nil {
			return nil, false, nil
		}
		return int(v.Unix()), true, nil
	case keys.AsyncEndTime:
		v := info.AsyncEndTime()
		return v, v != nil, nil
	case keys.AsyncEndUnixTimeInSec:
		v := info.AsyncEndTime()
		if v == nil {
			return nil, false, nil
		}
		return int(v.UnixMilli() / 1000), true, nil
	}
	return nil, false, nil
}
