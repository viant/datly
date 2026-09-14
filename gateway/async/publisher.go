// Package async adapts the original Datly .job storage-event transport to
// the durable jobs service. AFS owns local and cloud URL operations.
package async

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PublishConfig struct {
	FS           afs.Service
	Notification xasync.Notification
}
type Publisher struct {
	mu           sync.Mutex
	fs           afs.Service
	notification xasync.Notification
}

func NewPublisher(config PublishConfig) (*Publisher, error) {
	if config.Notification.Method != xasync.NotificationMethodStorage || config.Notification.Destination == "" {
		return nil, fmt.Errorf("async storage notification requires Method Storage and Destination")
	}
	if config.FS == nil {
		config.FS = afs.New()
	}
	return &Publisher{fs: config.FS, notification: config.Notification}, nil
}
func (p *Publisher) Prepare(record *jobs.Record) error {
	if record == nil || record.ID == "" {
		return fmt.Errorf("durable job identity is required")
	}
	destination := p.notification.Destination
	now := time.Now()
	if !strings.Contains(destination, "$") {
		view := record.MainView
		if view == "" {
			view = "jobs"
		}
		if path.Base(view) != view || view == "." || view == ".." {
			return fmt.Errorf("invalid job main view path")
		}
		destination = url.Join(destination, view, strconv.FormatInt(now.UnixMicro(), 10)+"_"+uuid.NewString()+".job")
	} else {
		destination = strings.NewReplacer("${unixUs}", strconv.FormatInt(now.UnixMicro(), 10), "${unixMs}", strconv.FormatInt(now.UnixMilli(), 10), "${viewName}", record.MainView, "${jobHash}", strconv.FormatInt(now.UnixMilli(), 10)).Replace(destination)
		if strings.Contains(destination, "${") {
			return fmt.Errorf("unsupported job destination marker")
		}
	}
	if !strings.HasSuffix(url.Path(destination), ".job") {
		return fmt.Errorf("storage job destination must end in .job")
	}
	record.EventURL = url.Normalize(destination, file.Scheme)
	return nil
}
func (p *Publisher) Publish(ctx context.Context, record *jobs.Record) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if record == nil || record.EventURL == "" {
		return fmt.Errorf("prepared job EventURL is required")
	}
	event, err := record.Event()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	exists, err := p.fs.Exists(ctx, record.EventURL)
	if err != nil {
		return err
	}
	if exists {
		data, err := p.fs.DownloadWithURL(ctx, record.EventURL)
		if err != nil {
			return err
		}
		var published jobs.Event
		if err = json.Unmarshal(data, &published); err == nil && published.ID == record.ID && published.Method == record.Method && published.URI == record.URI {
			return nil
		}
		return fmt.Errorf("job EventURL already belongs to a different or invalid event")
	}

	// Watchers only consume .job. The AFS move publishes a complete local file;
	// cloud object finalization/visibility remains owned by its AFS provider.
	staging := record.EventURL + "." + uuid.NewString() + ".upload"
	published := false
	defer func() {
		if !published {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			_ = p.fs.Delete(cleanup, staging)
		}
	}()
	if err = p.fs.Upload(ctx, staging, file.DefaultFileOsMode, bytes.NewReader(payload)); err != nil {
		return err
	}
	if err = p.fs.Move(ctx, staging, record.EventURL); err != nil {
		return err
	}
	published = true
	return nil
}
