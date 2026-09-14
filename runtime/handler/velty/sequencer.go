package velty

import (
	"context"
	"fmt"

	xhandler "github.com/viant/xdatly/handler"
)

type Sequencer struct {
	ctx     context.Context
	service xhandler.Sequencer
}

func (s *Sequencer) Allocate(tableName string, dest interface{}, selector string) (string, error) {
	if s == nil || s.service == nil {
		return "", fmt.Errorf("velty sequencer capability is not configured")
	}
	return "", s.service.Allocate(s.ctx, tableName, dest, selector)
}
