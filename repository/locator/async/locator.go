package async

import (
	"context"
	"github.com/viant/datly/repository/locator/async/keys"
	"github.com/viant/datly/service/operator/exec"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/response"
	"strings"
	"time"
)

type Locator struct{}

func (l *Locator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	name = strings.ToLower(name)
	switch {
	case strings.HasPrefix(name, keys.Group):
		return l.getGroupValue(ctx, name)
	case strings.HasPrefix(name, keys.Job):
		return l.getJobValue(ctx, name)
	}
	return nil, false, nil
}

func (l *Locator) getJobValue(ctx context.Context, name string) (interface{}, bool, error) {
	var job *async.Job
	if value := ctx.Value(async.JobKey); value != nil {
		job, _ = value.(*async.Job)
	}
	if job == nil {
		return nil, false, nil
	}
	if strings.HasPrefix(name, keys.JobInfo) {
		return l.getJobInfoValue(ctx, name, job)
	}
	switch name {
	case keys.Job:
		return job, true, nil
	case keys.JobCreationTime:
		return job.CreationTime, true, nil
	case keys.JobEndTime:
		val := job.EndTime
		return val, val != nil, nil
	case keys.JobUserEmail:
		return job.UserEmail, job.UserEmail != nil, nil
	case keys.JobUserID:
		return job.UserID, job.UserID != nil, nil
	case keys.JobEndUnixTimeInSec:
		val := job.EndTime
		if val == nil {
			return 0, false, nil
		}
		return int(val.Unix()), true, nil
	}
	return nil, false, nil
}

func (l *Locator) getJobInfoValue(ctx context.Context, name string, job *async.Job) (interface{}, bool, error) {
	var jobInfo response.JobInfo
	if strings.HasPrefix(name, keys.JobInfo) {
		jobInfo = buildJobInfo(job)
	}
	switch name {
	case keys.JobInfo:
		return jobInfo, true, nil
	case keys.JobInfoStatus:
		return jobInfo.JobStatus, true, nil
	case keys.JobInfoStatusCode: //alternative names of job status
		if job.Error != nil && *job.Error != "" {
			return "ERROR", true, nil
		}
		//TODO may add regular status check
		switch async.Status(jobInfo.JobStatus) {
		case async.StatusDone:
			return "COMPLETE", true, nil
		case async.StatusPending:
			return "WAITING", true, nil
		default:
			return jobInfo.JobStatus, true, nil
		}
	case keys.JobInfoCacheHit:
		return jobInfo.CacheHit, true, nil
	case keys.JobInfoCacheHits:
		if jobInfo.CacheHit {
			return 1, true, nil
		}
		return 0, true, nil
	case keys.JobInfoCacheKey:
		return jobInfo.CacheKey, true, nil
	case keys.JobInfoMatchKey:
		return jobInfo.MatchKey, true, nil
	case keys.JobInfoWaitTimeInMs:
		return jobInfo.WaitTimeInMs, true, nil
	case keys.JobInfoWaitTimeInSec:
		return jobInfo.WaitTimeInMs / 1000, true, nil
	case keys.JobInfoRunTimeInMs:
		return jobInfo.RunTimeInMs, true, nil
	case keys.JobInfoRunTimeInSec:
		return jobInfo.RunTimeInMs / 1000, true, nil
	case keys.JobInfoExpiryInSec:
		return jobInfo.ExpiryInSec, true, nil
	case keys.JobInfoPriority:
		return 0, true, nil
	}
	return nil, false, nil
}

func buildJobInfo(aJob *async.Job) response.JobInfo {
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

	jobStats := response.JobInfo{
		RequestTime:  time.Now(),
		JobStatus:    aJob.Status,
		CreateTime:   aJob.CreationTime,
		WaitTimeInMs: aJob.WaitTimeInMcs / 1000,
		RunTimeInMs:  aJob.RunTimeInMcs / 1000,
		ExpiryInSec:  expiryInSec,
		MatchKey:     aJob.MatchKey,
		CacheKey:     cacheKey,
		CacheHit:     cacheHit,
	}
	return jobStats
}

func (l *Locator) getGroupValue(ctx context.Context, name string) (interface{}, bool, error) {
	infoValue := ctx.Value(exec.ContextKey)
	if infoValue == nil {
		return nil, false, nil
	}
	info := infoValue.(*exec.Context)
	switch name {
	case keys.GroupStatus:
		return info.AsyncStatus(), true, nil
	case keys.GroupDone:
		return info.AsyncStatus() == string(async.StatusDone), true, nil
	case keys.GroupElapsedInSec:
		return int(info.AsyncElapsed().Seconds()), true, nil
	case keys.GroupElapsedInMs:
		return int(info.AsyncElapsed().Milliseconds()), true, nil
	case keys.GroupCreationTime:
		v := info.AsyncCreationTime()
		return v, v != nil, nil
	case keys.GroupCreationUnixTimeInSec:
		v := info.AsyncCreationTime()
		if v == nil {
			return nil, false, nil
		}
		return int(v.Unix()), true, nil
	case keys.GroupEndTime:
		v := info.AsyncEndTime()
		return v, v != nil, nil
	case keys.GroupEndUnixTimeInSec:
		v := info.AsyncEndTime()
		if v == nil {
			return nil, false, nil
		}
		return int(v.UnixMilli() / 1000), true, nil
	}
	return nil, false, nil
}

func (l *Locator) Names() []string {
	return nil
}

func newAsyncLocator(opts ...locator.Option) (kind.Locator, error) {
	ret := &Locator{}
	return ret, nil
}

func init() {
	locator.Register(state.KindAsync, newAsyncLocator)
}
