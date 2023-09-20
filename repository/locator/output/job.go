package output

import (
	"context"
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/response"
	"strings"
	"time"
)

func (l *outputLocator) getJobValue(ctx context.Context, name string) (interface{}, bool, error) {
	job := getJob(ctx)
	if job == nil {
		return nil, true, nil
	}

	var jobStatus response.JobStatus
	if strings.HasPrefix(name, "jobstatus") {
		jobStatus = buildJobStatus(job)
	}
	switch name {
	case "job":
		return job, true, nil
	case "jobstatus":
		return jobStatus, true, nil
	case "jobstatus.execstatus":
		return jobStatus.JobStatus, true, nil
	case "jobstatus.cachekey":
		return jobStatus.CacheKey, true, nil
	case "jobstatus.waittimemcs":
		return jobStatus.WaitTimeMcs, true, nil
	case "jobstatus.runtimemcs":
		return jobStatus.RunTimeMcs, true, nil
	case "jobstatus.expiryinsec":
		return jobStatus.ExpiryInSec, true, nil
	}
	return nil, false, nil
}

func buildJobStatus(aJob *async.Job) response.JobStatus {
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
	return jobStats
}

func getJob(ctx context.Context) *async.Job {
	if value := ctx.Value(async.JobKey); value != nil {
		ret, ok := value.(*async.Job)
		if ok {
			return ret
		}
	}
	return nil
}
