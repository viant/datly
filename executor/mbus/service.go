package mbus

import (
	"context"
	"fmt"
	"github.com/viant/cloudless/async/mbus"
)

type Service struct{}

func (s *Service) Push(dest *mbus.Resource, message *mbus.Message) (*mbus.Confirmation, error) {
	if dest.Vendor == "" {
		return nil, fmt.Errorf("vendor was empty")
	}
	srv := mbus.Lookup(dest.Vendor)
	if srv == nil {
		return nil, fmt.Errorf("unknow message bus vendor %v (forgot _ import)", dest.Vendor)
	}
	return srv.Push(context.Background(), dest, message)
}
