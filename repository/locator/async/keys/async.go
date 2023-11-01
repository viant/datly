package keys

const ( //async kind

	// Job releated keys
	Job = "job"

	JobCreationTime = "job.creationtime"

	JobEndTime = "job.endtime"

	JobUserEmail = "job.useremail"
	JobUserID    = "job.userid"
	JobError     = "job.error"

	JobEndUnixTimeInSec = "job.endunixtimeinsec"

	//JobInfo related keys

	JobInfo = "jobinfo"

	JobInfoStatus = "jobinfo.status"

	JobInfoStatusCode = "jobinfo.code"

	JobInfoCacheHit  = "jobinfo.cachehit"
	JobInfoCacheHits = "jobinfo.cachehits"

	JobInfoCacheKey = "jobinfo.cachekey"
	JobInfoPriority = "jobinfo.priority"

	JobInfoMatchKey      = "jobinfo.matchkey"
	JobInfoWaitTimeInMs  = "jobinfo.waittimeinms"
	JobInfoWaitTimeInSec = "jobinfo.waittimeinsec"
	JobInfoRunTimeInMs   = "jobinfo.runtimeinms"
	JobInfoRunTimeInSec  = "jobinfo.runtimeinsec"
	JobInfoExpiryInSec   = "jobinfo.expiryinsec"

	//Group releated keys

	Group = "group"

	GroupStatus = "group.status"

	GroupDone = "group.done"

	GroupElapsedInSec = "group.elapsedinsec"

	GroupElapsedInMs = "group.elapsedinms"

	GroupEndTime = "group.endtime"

	GroupEndUnixTimeInSec = "group.endunixtimeinsec"

	GroupCreationTime = "group.creationtime"

	GroupCreationUnixTimeInSec = "group.creationunixtimeinsec"
)
