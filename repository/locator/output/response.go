package output

import (
	"context"
	"github.com/viant/datly/service/dispatcher/exec"
)

func (l *outputLocator) getResponseValue(ctx context.Context, name string) (interface{}, bool, error) {
	infoValue := ctx.Value(exec.InfoKey)
	if infoValue == nil {
		return nil, false, nil
	}
	info := infoValue.(*exec.Info)

	switch name {
	case "response.elapsedInSec":
		return int(info.Elapsed().Seconds()), true, nil
	case "response.time":
		return info.EndTime(), true, nil
	}
	return nil, false, nil
}
