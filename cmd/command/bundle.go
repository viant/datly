package command

import (
	"context"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/matcher"
	soption "github.com/viant/afs/option"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
)

func (s *Service) BundleRules(ctx context.Context, bundle *options.Bundle) error {
	cacheSetting := soption.WithCache(gateway.PackageFile, "gzip")
	return cache.Package(context.Background(), bundle.Source, bundle.RuleDest,
		cacheSetting,
		matcher.WithExtExclusion(".so", "so", ".gz", "gz"),
	)
}
