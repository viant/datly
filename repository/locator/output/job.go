package output

import (
	"context"
	"github.com/viant/datly/repository/locator/output/keys"
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
	var jobInfo response.JobInfo
	if strings.HasPrefix(name, keys.JobInfo) {
		jobInfo = buildJobInfo(job)
	}
	switch name {
	case keys.Job:
		return job, true, nil
	case keys.JobCreationTime:
		return job.CreationTime, true, nil
	case keys.JobEndTime:
		val := job.EndTime
		return val, val != nil, nil
	case keys.JobEndUnixTimeInSec:
		val := job.EndTime
		if val == nil {
			return 0, false, nil
		}
		return int(val.Unix()), true, nil
	case keys.JobInfo:
		return jobInfo, true, nil
	case keys.JobInfoStatus:
		return jobInfo.JobStatus, true, nil

	case keys.JobInfoStatusCode: //alternative names of job status
		if job.Error != nil && *job.Error != "" {
			return "ERROR", true, nil
		}
		switch async.Status(jobInfo.JobStatus) {
		case async.StatusDone:
			return "COMPLETED", true, nil
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

func getJob(ctx context.Context) *async.Job {
	if value := ctx.Value(async.JobKey); value != nil {
		ret, ok := value.(*async.Job)
		if ok {
			return ret
		}
	}
	return nil
}
