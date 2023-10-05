package command

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/warmup"
)

func (s *Service) WarmupCache(ctx context.Context, cache *options.CacheWarmup) error {
	srv, err := s.run(ctx, &cache.Run)
	if err != nil {
		return err
	}
	response := warmup.PreCache(ctx, srv.Service.PreCachables, cache.WarmupURIs...)
	data, _ := json.Marshal(response)
	fmt.Printf("%s\n", data)
	return nil
}
