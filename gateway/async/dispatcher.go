package async

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	afsstorage "github.com/viant/afs/storage"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/jobs"
	"reflect"
)

// JobHandler accepts durable events through the existing job service or its
// application admission owner.
type JobHandler interface {
	HandleJob(context.Context, *jobs.Event) (any, error)
}

type Dispatcher struct {
	fs   afs.Service
	jobs JobHandler
}

func NewDispatcher(fs afs.Service, service JobHandler) (*Dispatcher, error) {
	if service == nil {
		return nil, fmt.Errorf("storage dispatch requires a jobs service")
	}
	value := reflect.ValueOf(service)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, fmt.Errorf("storage dispatch requires a jobs service")
		}
	}
	if fs == nil {
		fs = afs.New()
	}
	return &Dispatcher{fs: fs, jobs: service}, nil
}

// DispatchStorageEvent accepts the same AFS storage.Object produced by local,
// S3 or GS adapters. It never opens an HTTP loopback connection.
func (d *Dispatcher) DispatchStorageEvent(ctx context.Context, object afsstorage.Object) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = dexec.NewPanicError("storage job dispatch", recovered)
		}
	}()
	if object == nil || object.IsDir() {
		return fmt.Errorf("job storage event requires a file object")
	}
	data, err := d.fs.Download(ctx, object)
	if err != nil {
		return err
	}
	var event jobs.Event
	if err = json.Unmarshal(data, &event); err != nil {
		return fmt.Errorf("decode job event: %w", err)
	}
	event.EventURL = object.URL()
	_, err = d.jobs.HandleJob(ctx, &event)
	return err
}
