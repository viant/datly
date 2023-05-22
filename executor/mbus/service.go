package mbus

import (
	"context"
	"fmt"
	"github.com/viant/cloudless/async/mbus"
	"strings"
)

type Service struct{}

func (s *Service) Push(message *mbus.Message) (*mbus.Confirmation, error) {
	dest := message.Resource
	if dest.Vendor == "" {
		return nil, fmt.Errorf("vendor was empty")
	}
	srv := mbus.Lookup(dest.Vendor)
	if srv == nil {
		return nil, fmt.Errorf("unknow message bus vendor %v (forgot _ import)", dest.Vendor)
	}
	return srv.Push(context.Background(), dest, message)
}

func (s *Service) Message(dest string, data interface{}) (*mbus.Message, error) {
	parts := strings.Split(dest, "/")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid dest format: expect: vendor/resourceType/region/nameOrURI")
	}
	var vendor, resourceType, region = parts[0], parts[1], parts[2]
	resource := &mbus.Resource{
		Vendor: vendor,
		Type:   resourceType,
		Region: region,
	}
	URI := strings.Join(parts[3:], "/")
	if len(parts) > 4 {
		resource.URL = URI
		resource.Name = parts[len(parts)-1]
	} else {
		resource.Name = URI
	}
	return &mbus.Message{Data: data, Resource: resource}, nil
}
