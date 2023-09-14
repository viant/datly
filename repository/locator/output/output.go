package output

import (
	"context"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/response"
	"strings"
	"time"
)

type outputLocator struct {
	Output           *reader.Output
	Status           *response.Status
	OutputParameters state.NamedParameters
	View             *view.View
	Metrics          reader.Metrics
}

func (l *outputLocator) Names() []string {
	return nil
}

func (l *outputLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	switch aName := strings.ToLower(name); aName {
	case "job":
		if value := ctx.Value(async.JobKey); value != nil {
			ret, ok := value.(*async.Job)
			if ok {
				return ret, true, nil
			}
		}
		return nil, false, nil
	case "jobstatus":
		if value := ctx.Value(async.JobKey); value != nil {
			aJob, ok := value.(*async.Job)
			if !ok {
				return nil, true, nil
			}
			expiryInSec := 0
			if expiryTime := aJob.ExpiryTime; expiryTime != nil {
				expiry := expiryTime.Sub(time.Now())
				expiryInSec = int(expiry.Seconds())
			}

			cacheKey := ""
			cacheHit := false
			if aJob.CacheKey != nil {
				cacheKey = *aJob.CacheKey
				cacheHit = true
			}

			jobStats := response.JobStatus{
				RequestTime: time.Now(),
				JobStatus:   aJob.Status,
				CreateTime:  aJob.CreationTime,
				WaitTimeMcs: aJob.WaitTimeMcs,
				RunTimeMcs:  aJob.RunTimeMcs,
				ExpiryInSec: expiryInSec,
				CacheKey:    cacheKey,
				CacheHit:    cacheHit,
			}
			return jobStats, true, nil
		}
		return nil, false, nil
	case "blabla":
		if value := ctx.Value(async.JobKey); value != nil {
			aJob, ok := value.(*async.Job)
			if !ok {
				return nil, true, nil
			}
			expiryInSec := 0
			if expiryTime := aJob.ExpiryTime; expiryTime != nil {
				expiry := expiryTime.Sub(time.Now())
				expiryInSec = int(expiry.Seconds())
			}

			cacheKey := ""
			cacheHit := false
			if aJob.CacheKey != nil {
				cacheKey = *aJob.CacheKey
				cacheHit = true
			}

			jobStats := response.JobStatus{
				RequestTime: time.Now(),
				JobStatus:   aJob.Status,
				CreateTime:  aJob.CreationTime,
				WaitTimeMcs: aJob.WaitTimeMcs,
				RunTimeMcs:  aJob.RunTimeMcs,
				ExpiryInSec: expiryInSec,
				CacheKey:    cacheKey,
				CacheHit:    cacheHit,
			}
			return jobStats, true, nil
		}
		return nil, false, nil
	case "jobstatus.waittimemcs", "jobstatus.runtimemcs", "jobstatus.expiryinsec":
		//return l.View.Name, true, nil
		if value := ctx.Value(async.JobKey); value != nil {
			aJob, ok := value.(*async.Job)
			if !ok {
				return nil, true, nil
			}
			expiryInSec := 0
			if expiryTime := aJob.ExpiryTime; expiryTime != nil {
				expiry := expiryTime.Sub(time.Now())
				expiryInSec = int(expiry.Seconds())
			}

			cacheKey := ""
			cacheHit := false
			if aJob.CacheKey != nil {
				cacheKey = *aJob.CacheKey
				cacheHit = true
			}

			jobStats := response.JobStatus{
				RequestTime: time.Now(),
				JobStatus:   aJob.Status,
				CreateTime:  aJob.CreationTime,
				WaitTimeMcs: aJob.WaitTimeMcs,
				RunTimeMcs:  aJob.RunTimeMcs,
				ExpiryInSec: expiryInSec,
				CacheKey:    cacheKey,
				CacheHit:    cacheHit,
			}
			switch aName {
			case "jobstatus.waittimemcs":
				return jobStats.WaitTimeMcs, true, nil
			case "jobstatus.runtimemcs":
				return jobStats.RunTimeMcs, true, nil
			case "jobstatus.expiryinsec":
				return jobStats.ExpiryInSec, true, nil
			case "jobstatus.cachekey":
				return jobStats.CacheKey, true, nil
			}

		}
		return nil, false, nil
	case "data":
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.Data, true, nil
	case "summary", "meta":
		if l.Output == nil {
			return nil, false, nil
		}
		return l.Output.ViewMeta, true, nil
	case "status":
		if l.Status == nil {
			return nil, false, nil
		}
		return l.Status, true, nil
	case "sql":
		if l.Output == nil {
			return nil, false, nil
		}
		SQL := l.Output.Metrics.SQL()
		return SQL, true, nil
	case "view.name":
		return l.View.Name, true, nil
	case "view.id":
		return l.View.Name, true, nil
	case "filter":
		parameter := l.OutputParameters.LookupByLocation(state.KindOutput, "filter")
		if parameter == nil || l.Output == nil {
			return nil, false, nil
		}
		filterState, err := l.buildFilter(parameter)
		if err != nil {
			return nil, false, err
		}
		return filterState.State(), true, nil
	}
	return nil, false, nil
}

// newOutputLocator returns output locator
func newOutputLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	ret := &outputLocator{OutputParameters: options.OutputParameters}
	for _, candidate := range options.Custom {
		if output, ok := candidate.(*reader.Output); ok {
			ret.Output = output
		}
		if status, ok := candidate.(*response.Status); ok {
			ret.Status = status
		}
	}
	ret.View = options.View
	ret.Metrics = options.Metrics
	return ret, nil
}

func init() {
	locator.Register(state.KindOutput, newOutputLocator)
}
