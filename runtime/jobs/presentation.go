package jobs

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
	xasync "github.com/viant/xdatly/async"
	xresponse "github.com/viant/xdatly/response"
)

// Presentation supplies original Datly KindAsync metadata for one durable job.
// It holds public detached metadata, never raw SourceState or a principal service.
type Presentation struct {
	Job   *xasync.Job
	Error error
}

func (p *Presentation) Kind() string                              { return "async" }
func (p *Presentation) Priority() int                             { return locator.PrioritySource }
func (p *Presentation) DefaultCacheable() bool                    { return false }
func (p *Presentation) Locate(*structology.State) locator.Locator { return p }
func (p *Presentation) Value(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
	job := p.Job
	if job == nil {
		return nil, false, nil
	}
	switch strings.ToLower(name) {
	case "job":
		return job, true, nil
	case "job.creationtime":
		return job.CreationTime, true, nil
	case "job.endtime":
		return job.EndTime, job.EndTime != nil, nil
	case "job.endunixtimeinsec":
		if job.EndTime == nil {
			return nil, false, nil
		}
		return int(job.EndTime.Unix()), true, nil
	case "job.userid":
		return job.UserID, job.UserID != nil, nil
	case "job.useremail":
		return job.UserEmail, job.UserEmail != nil, nil
	case "job.error":
		if p.Error != nil {
			return p.Error.Error(), true, nil
		}
		if job.Error != nil {
			return *job.Error, true, nil
		}
		return "", true, nil
	case "jobinfo.status", "group.status":
		return string(job.Status), true, nil
	case "jobinfo.code":
		if job.Error != nil && *job.Error != "" {
			return "ERROR", true, nil
		}
		switch job.Status {
		case xasync.StatusDone:
			return "COMPLETE", true, nil
		case xasync.StatusPending:
			return "WAITING", true, nil
		}
		return string(job.Status), true, nil
	case "jobinfo.matchkey":
		return job.MatchKey, true, nil
	case "jobinfo.waittimeinms":
		return job.WaitTimeInMcs / 1000, true, nil
	case "jobinfo.waittimeinsec":
		return job.WaitTimeInMcs / 1000000, true, nil
	case "jobinfo.runtimeinms":
		return job.RunTimeInMcs / 1000, true, nil
	case "jobinfo.runtimeinsec":
		return job.RunTimeInMcs / 1000000, true, nil
	case "jobinfo.expiryinsec":
		if job.ExpiryTime == nil {
			return 0, true, nil
		}
		return int(time.Until(*job.ExpiryTime).Seconds()), true, nil
	case "jobinfo.priority":
		return 0, true, nil
	case "jobinfo.cachekey":
		if job.CacheKey == nil {
			return "", true, nil
		}
		return *job.CacheKey, true, nil
	case "jobinfo.cachehit":
		return job.CacheKey != nil, true, nil
	case "jobinfo.cachehits":
		if job.CacheKey == nil {
			return 0, true, nil
		}
		return 1, true, nil
	case "group.done":
		return job.Status == xasync.StatusDone, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported async output metadata %q", name)
	}
}

// StatusPresentation supplies the existing output/status shape for metadata-only
// replies. Actual handler result finalization remains inside the execution engine.
type StatusPresentation struct{ Error error }

func (p *StatusPresentation) Kind() string                              { return "output" }
func (p *StatusPresentation) Priority() int                             { return locator.PrioritySource }
func (p *StatusPresentation) DefaultCacheable() bool                    { return false }
func (p *StatusPresentation) Locate(*structology.State) locator.Locator { return p }
func (p *StatusPresentation) Value(_ context.Context, target reflect.Type, name string) (any, bool, error) {
	if name != "status" {
		return nil, false, nil
	}
	status := xresponse.Status{Status: "ok"}
	if p.Error != nil {
		status.Status = "error"
		status.Message = p.Error.Error()
		status.Error = p.Error.Error()
	}
	if target != nil && target.Kind() == reflect.String {
		return status.Status, true, nil
	}
	return status, true, nil
}
