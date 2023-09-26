package output

import (
	"context"
	"github.com/viant/datly/repository/locator/output/keys"
	"github.com/viant/datly/service/dispatcher/exec"
)

func (l *outputLocator) getResponseValue(ctx context.Context, name string) (interface{}, bool, error) {
	infoValue := ctx.Value(exec.InfoKey)
	if infoValue == nil {
		return nil, false, nil
	}
	info := infoValue.(*exec.Info)

	switch name {
	case keys.ResponseElapsedInSec:
		return int(info.AsyncElapsed().Seconds()), true, nil
	case keys.ResponseElapsedInMs:
		return int(info.Elapsed().Milliseconds()), true, nil
	case keys.ResponseUnixTimeInSec:
		return info.EndTime().Unix(), true, nil
	case keys.ResponseTime:
		//return info.AsyncEndTime(), true, nil
		return info.EndTime(), true, nil

	}
	return nil, false, nil
}
