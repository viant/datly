package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/xdatly/handler/async"
	"log"
	"time"
)

func (s *Service) watchAsyncJob(ctx context.Context) {
	if s.Config.JobURL == "" {
		return
	}
	var limiter chan bool
	if s.Config.MaxJobs > 0 {
		limiter = make(chan bool, s.Config.MaxJobs)
	}
	for {
		objects, _ := s.fs.List(ctx, s.Config.JobURL, option.NewRecursive(true))
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

		for i, object := range objects {
			if object.IsDir() {
				continue
			}
			if limiter != nil {
				limiter <- true
			}

			go func(object storage.Object) {
				defer func() {
					if limiter != nil {
						<-limiter
					}
				}()
				router, _ := s.Router()
				if router != nil {
					err := router.DispatchStorageEvent(context.Background(), object)
					if err != nil {
						log.Println(err)
					}
					if err == nil {
						err = fs.Delete(ctx, object.URL())
					} else {
						destURL := url.Join(s.Config.FailedJobURL, time.Now().Format("20060102"), object.Name())
						err = fs.Move(ctx, object.URL(), destURL)
					}

					if err != nil {
						log.Println(err)
					}

				} else {
					log.Println("router was nil")
				}
			}(objects[i])
		}

	}
}

func (s *Service) handleJobEvent(ctx context.Context, object storage.Object, router *Router) error {
	data, err := s.fs.Download(ctx, object)
	if err != nil {
		return err
	}
	job := &async.Job{}
	if err = json.Unmarshal(data, job); err != nil {
		return err
	}
	job.EventURL = object.URL()
	return router.HandleJob(ctx, job)
}
