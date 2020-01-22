package singleton

import (
	"context"
	"github.com/pkg/errors"
	rconfig "github.com/viant/datly/config"
	"github.com/viant/datly/reader"
)

var readerService reader.Service

//Reader returns reader service singleton
func Reader(ctx context.Context, source string) (reader.Service, error) {
	if readerService != nil {
		return readerService, nil
	}
	config, err := rconfig.NewConfig(ctx, source)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create config for %v", source)
	}
	readerService, err := reader.New(ctx, config)
	return readerService, err
}
