package keys

import (
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xreflect"
	"reflect"
)

var Types = map[string]reflect.Type{
	//Job types
	Job:                  reflect.TypeOf(&async.Job{}),
	JobCreationTime:      xreflect.TimeType,
	JobError:             xreflect.StringType,
	JobEndTime:           xreflect.TimePtrType,
	JobEndUnixTimeInSec:  xreflect.IntType,
	JobInfoStatus:        xreflect.StringType,
	JobInfoStatusCode:    xreflect.StringType,
	JobInfoMatchKey:      xreflect.StringType,
	JobInfoWaitTimeInMs:  xreflect.IntType,
	JobInfoWaitTimeInSec: xreflect.IntType,
	JobInfoRunTimeInMs:   xreflect.IntType,
	JobInfoRunTimeInSec:  xreflect.IntType,
	JobInfoExpiryInSec:   xreflect.IntType,
	JobInfoPriority:      xreflect.IntType,
	JobInfoCacheHit:      xreflect.BoolType,
	JobInfoCacheHits:     xreflect.IntType,

	GroupStatus:       xreflect.StringType,
	GroupDone:         xreflect.BoolType,
	GroupElapsedInSec: xreflect.IntType,
	GroupElapsedInMs:  xreflect.IntType,

	GroupCreationTime:          xreflect.TimePtrType,
	GroupCreationUnixTimeInSec: xreflect.IntType,
	GroupEndTime:               xreflect.TimePtrType,
	GroupEndUnixTimeInSec:      xreflect.IntType,
}
