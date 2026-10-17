package standalone

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/viant/datly/application"
	"github.com/viant/datly/typecatalog"
)

// Compile prepares linked components with the standalone connector, codec and
// provider configuration, without publishing a manager generation or opening
// listeners. The result can be composed into another application's generation.
// It eagerly compiles only Config.GoBootstrap.Packages. The returned Shutdown
// owns all prepared connections and cache clients and must be retained by the
// composing build, including when composition or publication fails.
func Compile(ctx context.Context, options Options) (_ *application.Build, err error) {
	if options.Config == nil || options.Config.GoBootstrap == nil || len(options.Config.GoBootstrap.Packages) == 0 {
		return nil, fmt.Errorf("linked compilation requires explicit component packages")
	}
	server, err := New(ctx, options)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, server.Shutdown(context.Background()))
		}
	}()
	build, err := server.source.compileEager(ctx, typecatalog.NewCatalog())
	if err != nil {
		return nil, err
	}
	shutdown := build.Shutdown
	var once sync.Once
	var closeErr error
	build.Shutdown = func(ctx context.Context) error {
		once.Do(func() {
			if shutdown != nil {
				closeErr = shutdown(ctx)
			}
			closeErr = errors.Join(closeErr, server.Shutdown(ctx))
		})
		return closeErr
	}
	return build, nil
}
