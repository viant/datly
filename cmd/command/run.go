package command

import (
	"context"
	"encoding/json"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/service/auth/jwt"
	"github.com/viant/xdatly/handler/async"
	"log"
	"sync"
	"time"
)

func (s *Service) Run(ctx context.Context, run *options.Run) (err error) {
	srv, err := s.run(ctx, run)
	if err != nil {
		return err
	}
	if run.JobURL != "" {

		go s.dispatchEventsIfNeeded(context.Background(), run, srv)
	}

	return srv.ListenAndServe()
}

func (s *Service) dispatchEventsIfNeeded(ctx context.Context, run *options.Run, srv *standalone.Server) {
	for {
		objects, _ := s.fs.List(ctx, run.JobURL, option.NewRecursive(true))
		objectCount := 0
		for _, object := range objects {
			if object.IsDir() {
				continue
			}
			objectCount++
		}
		if objectCount == 0 {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		wg := sync.WaitGroup{}
		for i, object := range objects {
			if object.IsDir() {
				continue
			}
			wg.Add(1)
			go func(object storage.Object) {
				defer wg.Done()
				err := s.dispatchEvent(context.Background(), object, srv)
				if err != nil {
					log.Println(err)
				}
				_ = s.fs.Delete(ctx, object.URL())

			}(objects[i])
		}
		wg.Wait()
	}
}

func (s *Service) dispatchEvent(ctx context.Context, object storage.Object, srv *standalone.Server) error {
	data, err := s.fs.Download(ctx, object)
	if err != nil {
		return err
	}
	job := &async.Job{}
	if err = json.Unmarshal(data, job); err != nil {
		return err
	}
	router, _ := srv.Service.Router()
	return router.HandleJob(ctx, job)
}

func (s *Service) run(ctx context.Context, run *options.Run) (*standalone.Server, error) {
	var err error
	if s.config, err = standalone.NewConfigFromURL(ctx, run.ConfigURL); err != nil {
		return nil, err
	}
	authenticator, err := jwt.Init(s.config.Config, nil)
	var srv *standalone.Server
	if authenticator == nil {
		srv, err = standalone.New(ctx, s.config)
	} else {
		srv, err = standalone.NewWithAuth(ctx, s.config, authenticator)
	}
	return srv, err
}
