package keys

import (
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xreflect"
	"reflect"
)

const ( //Components keys
	//Data represents component view response
	Data = "data"

	//SQL represents component expanded SQL
	SQL = "sql"

	//Status represents response status output key
	Status = "status"

	Error = "error"

	StatusCode = "status.code"

	//Summary represents view summary output key
	Summary = "summary"

	//SummaryMeta legacy, alternative SummaryKey
	SummaryMeta     = "meta"
	ViewName        = "view.name"
	ViewDescription = "view.description"
)

const ( //Job/Async keys
	Job = "job"

	JobInfo = "jobinfo"

	JobInfoStatus = "jobinfo.status"

	JobInfoStatusCode = "jobinfo.code"

	JobInfoCacheKey = "jobinfo.cachekey"
	JobInfoPriority = "jobinfo.priority"

	JobInfoMatchKey      = "jobinfo.matchkey"
	JobInfoWaitTimeInMs  = "jobinfo.waittimeinms"
	JobInfoWaitTimeInSec = "jobinfo.waittimeinsec"
	JobInfoRunTimeInMs   = "jobinfo.runtimeinms"
	JobInfoRunTimeInSec  = "jobinfo.runtimeinsec"
	JobInfoExpiryInSec   = "jobinfo.expiryinsec"

	AsyncStatus = "async.status"

	AsyncDone = "async.done"

	AsyncElapsedInSec = "async.elapsedinsec"

	AsyncElapsedInMs = "async.elapsedinms"

	AsyncEndTime = "async.endtime"

	AsyncEndUnixTimeInSec = "async.endunixtimeinsec"

	AsyncCreationTime = "async.creationtime"

	AsyncCreationUnixTimeInSec = "async.creationunixtimeinsec"
)

const ( //Response keys
	ResponseElapsedInSec = "response.elapsedinsec"

	ResponseElapsedInMs = "response.elapsedinms"

	ResponseTime = "response.time"

	ResponseUnixTimeInSec = "response.unixtimeinsec"
)

var Types = map[string]reflect.Type{
	//Component/View related keys
	Status:          reflect.TypeOf(response.Status{}),
	SQL:             xreflect.StringType,
	ViewName:        xreflect.StringType,
	ViewDescription: xreflect.StringType,

	//Job types
	Job:                  reflect.TypeOf(&async.Job{}),
	JobInfoStatus:        xreflect.StringType,
	JobInfoStatusCode:    xreflect.StringType,
	JobInfoMatchKey:      xreflect.StringType,
	JobInfoWaitTimeInMs:  xreflect.IntType,
	JobInfoWaitTimeInSec: xreflect.IntType,
	JobInfoRunTimeInMs:   xreflect.IntType,
	JobInfoRunTimeInSec:  xreflect.IntType,
	JobInfoExpiryInSec:   xreflect.IntType,
	JobInfoPriority:      xreflect.IntType,

	AsyncStatus:       xreflect.StringType,
	AsyncDone:         xreflect.BoolType,
	AsyncElapsedInSec: xreflect.IntType,
	AsyncElapsedInMs:  xreflect.IntType,

	AsyncCreationTime:          xreflect.TimePtrType,
	AsyncCreationUnixTimeInSec: xreflect.IntType,
	AsyncEndTime:               xreflect.TimePtrType,
	AsyncEndUnixTimeInSec:      xreflect.IntType,

	//Response keys
	ResponseTime: xreflect.TimeType,

	ResponseElapsedInSec: xreflect.IntType,
	ResponseElapsedInMs:  xreflect.IntType,
}
