package gateway

import (
	"context"
	"encoding/json"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/xdatly/handler/async"
	"log"
	"sync"
	"time"
)

func (r *Service) watchAsyncJob(ctx context.Context) {
	if r.Config.JobURL == "" {
		return
	}
	var limiter chan bool
	if r.Config.MaxJobs > 0 {
		limiter = make(chan bool, r.Config.MaxJobs)
	}
	pending := make(map[string]bool)
	var mux sync.RWMutex
	for {
		objects, _ := r.fs.List(ctx, r.Config.JobURL, option.NewRecursive(true))
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
			mux.RLock()
			isPending := pending[object.URL()]
			mux.RUnlock()
			if isPending {
				continue
			}
			mux.Lock()
			pending[object.URL()] = true
			mux.Unlock()

			if limiter != nil {
				limiter <- true
			}

			go func(object storage.Object) {
				defer func() {
					if limiter != nil {
						<-limiter
					}
					mux.Lock()
					delete(pending, object.URL())
					mux.Unlock()
				}()
				router, _ := r.Router()
				if router != nil {
					err := router.DispatchStorageEvent(context.Background(), object)
					if err != nil {
						log.Println(err)
					}
					if err == nil {
						err = fs.Delete(ctx, object.URL())
					} else {
						destURL := url.Join(r.Config.FailedJobURL, time.Now().Format("20060102"), object.Name())
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
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *Service) handleJobEvent(ctx context.Context, object storage.Object, router *Router) error {
	data, err := r.fs.Download(ctx, object)
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
