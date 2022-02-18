package singleton

import (
	"context"
	"github.com/pkg/errors"
	rconfig "github.com/viant/datly/v0/config"
	"github.com/viant/datly/v0/reader"
)

var readerService reader.Service

func Singleton(ctx context.Context, config *rconfig.Config) (reader.Service, error) {
	if readerService != nil {
		return readerService, nil
	}
	readerService, err := reader.New(ctx, config)
	return readerService, err
}

//Reader returns reader service singleton
func Reader(ctx context.Context, source string) (reader.Service, error) {
	if readerService != nil {
		return readerService, nil
	}
	config, err := rconfig.NewConfig(ctx, source)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create config for %v", source)
	}
	return Singleton(ctx, config)
}
