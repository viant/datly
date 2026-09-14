package http

import (
	"context"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/spec"
)

// AsyncRoute explicitly enables original default scheduling for an exact public
// route. Controls name existing canonical input fields, never HTTP parameter
// locations. No implicit async/sync query flag or arbitrary target is accepted.
type AsyncRoute struct {
	Route    spec.RouteRef
	MatchKey string
	SyncFlag string
	// Inspect configures a separate authored route to inspect exact stored IDs.
	// Its input contract still verifies any declared JWT before status authorization.
	Inspect *AsyncInspect
}

// Result requires a real completed reader and current target query/body/JWT
// sources. False is status-only and never invokes the reader. SyncFlag may be
// configured on a result route to explicitly refresh instead of cache-only lookup.
type AsyncInspect struct {
	Result bool
	JobID  string
	Target spec.RouteRef
}

// AsyncService is supplied by application.Manager with its generation and owned
// admission lifetime. It delegates durable work to the existing jobs.Service.
type AsyncService interface {
	Exchange(context.Context, jobs.Submission) (*jobs.Exchange, error)
}

// AsyncAdmission borrows Manager's already-pinned generation for this HTTP
// request and tracks preparation, execution and response handling until release.
type AsyncAdmission interface {
	Begin(context.Context) (context.Context, AsyncService, func(), error)
}
