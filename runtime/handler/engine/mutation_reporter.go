package engine

import (
	"context"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/handler/provider"
)

// Keep transaction completion and database access behind the data scope.
type mutationReporter struct{ service dexec.MutationReporter }

func (r mutationReporter) MutationReport() dexec.MutationReport {
	report := r.service.MutationReport()
	report.Results = append([]dexec.MutationResult(nil), report.Results...)
	return report
}

func (s *dataScope) mutationReporterProvider() locator.Provider {
	return provider.New(dexec.MutationReporterKey, func(ctx context.Context) (any, bool, error) {
		data, err := s.resolve(ctx)
		if err != nil || data == nil {
			return nil, false, err
		}
		reporter, ok := data.(dexec.MutationReporter)
		if !ok {
			return nil, false, nil
		}
		return mutationReporter{service: reporter}, true, nil
	})
}
