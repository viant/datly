package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	dexec "github.com/viant/datly/exec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	xasync "github.com/viant/xdatly/async"
	xresponse "github.com/viant/xdatly/response"
)

// ResultReader is the canonical dispatcher's optional reader capability. It
// builds a new authorized typed read, never repeats a mutation or decodes output.
type ResultReader interface {
	ReadResult(context.Context, ResultRequest) (any, error)
}
type ResultRequest struct {
	Job         *Record
	Source      dexec.ProviderScope
	SourceState string
	Refresh     bool
}

// readResult uses the same durable row snapshot that passed inspection. It
// neither rereads a differently authorized row nor performs redundant SQL reads.
func (s *Service) readResult(ctx context.Context, request ResultRequest) (any, error) {
	record := request.Job
	if record.Status != xasync.StatusDone {
		return nil, ErrInProgress
	}
	if record.Deactivated || record.JobType != "Reader" {
		return nil, ErrResultUnavailable
	}
	if record.ExpiryTime != nil && !record.ExpiryTime.After(time.Now()) {
		return nil, cache.ErrMiss
	}
	reader, ok := s.dispatcher.(ResultReader)
	if !ok {
		return nil, ErrResultUnavailable
	}
	return reader.ReadResult(ctx, request)
}

// QueryScope projects original durable execution metadata to a native SQLX
// identity guard. SQL is only compared with newly built SQL, never executed here.
func (r *Record) QueryScope() (*sqlxread.QueryScope, error) {
	var recorded []*xresponse.ParametrizedSQL
	decoder := json.NewDecoder(strings.NewReader(r.SQLQuery))
	decoder.UseNumber()
	if err := decoder.Decode(&recorded); err != nil {
		return nil, fmt.Errorf("job read query metadata: %w", err)
	}
	queries := make([]cache.ParmetrizedQuery, 0, len(recorded))
	for _, query := range recorded {
		if query != nil {
			queries = append(queries, cache.ParmetrizedQuery{SQL: query.Query, Args: query.Args})
		}
	}
	return sqlxread.NewQueryScope(queries)
}
