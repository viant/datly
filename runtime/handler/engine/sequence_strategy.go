package engine

import (
	"context"
	"errors"
	"fmt"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
)

var ErrSequenceStrategyConflict = errors.New("component sequence strategy conflicts with the root database unit")

type sequenceStrategySource interface {
	WithSequenceStrategy(string) (dexec.DataSource, error)
}
type sequenceStrategyIdentity interface{ InvocationSequenceStrategy() string }

type sequenceStrategyResolver interface {
	ResolveSequenceStrategy(context.Context) (dexec.DataSource, string, error)
}

// Resolve once before the unit is opened (or first compared). Both comparison
// and Open use this same frozen source/policy, never a child's requested policy.
func (s *dataScope) effectiveSequenceStrategy(ctx context.Context) (string, error) {
	s.sequenceOnce.Do(func() {
		source, err := s.configuredSequenceSource()
		if err != nil {
			s.sequenceError = err
			return
		}
		resolved := s.sequenceStrategy
		if resolved == "" {
			if configured, ok := source.(sequenceStrategyIdentity); ok {
				resolved = configured.InvocationSequenceStrategy()
			}
		}
		if resolver, ok := source.(sequenceStrategyResolver); ok {
			prepared, policy, err := resolver.ResolveSequenceStrategy(ctx)
			if err != nil {
				s.sequenceError = err
				return
			}
			if !sameIdentity(sourceKey(prepared), sourceKey(source)) || !sameIdentity(sourceTransactionKey(prepared), sourceTransactionKey(source)) {
				s.sequenceError = fmt.Errorf("effective sequence strategy resolution changed data source ownership")
				return
			}
			source, resolved = prepared, policy
		}
		s.sequenceSource, s.sequenceResolved = source, resolved
	})
	return s.sequenceResolved, s.sequenceError
}
func (s *dataScope) checkSequenceStrategy(ctx context.Context, source dexec.DataSource, requested string) error {
	if requested == "" {
		if configured, ok := source.(sequenceStrategyIdentity); ok {
			requested = configured.InvocationSequenceStrategy()
		}
	}
	if requested == "" {
		return nil
	}
	effective, err := s.effectiveSequenceStrategy(ctx)
	if err != nil {
		return err
	}
	if requested != effective {
		return fmt.Errorf("%w: child %q, root %q", ErrSequenceStrategyConflict, requested, effective)
	}
	return nil
}
func (s *dataScope) configuredSequenceSource() (dexec.DataSource, error) {
	if err := (&spec.Settings{SequenceStrategy: s.sequenceStrategy}).ValidateSequenceStrategy(); err != nil {
		return nil, err
	}
	if s.sequenceStrategy == "" {
		return s.source, nil
	}
	configurable, ok := s.source.(sequenceStrategySource)
	if !ok {
		return nil, fmt.Errorf("data source does not support sequence_strategy %q", s.sequenceStrategy)
	}
	configured, err := configurable.WithSequenceStrategy(s.sequenceStrategy)
	if err != nil {
		return nil, err
	}
	if !sameIdentity(sourceKey(configured), sourceKey(s.source)) || !sameIdentity(sourceTransactionKey(configured), sourceTransactionKey(s.source)) {
		return nil, fmt.Errorf("sequence strategy configuration changed data source ownership")
	}
	return configured, nil
}
