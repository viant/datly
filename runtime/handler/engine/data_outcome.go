package engine

import xhandler "github.com/viant/xdatly/handler"

// Data implementations opt into evidence-based reporting independently from
// their existing Complete/Flush lifecycle. No successful error return implies
// a database commit when this capability is absent.
type transactionOutcomeReporter interface {
	TransactionOutcome() xhandler.TransactionOutcome
}

func (s *dataScope) recordCompletion(units []*dataScope, err error) {
	result := xhandler.Outcome{Error: err, Transactions: make([]xhandler.TransactionOutcome, 0, len(units))}
	for index, unit := range units {
		state := xhandler.TransactionOutcome{Unit: index, State: xhandler.TransactionUnknown}
		if unit.err != nil {
			// Failed ownership/startup cannot borrow a reused Data object's
			// report from an earlier invocation.
			state.Error = unit.err
		} else if reporter, ok := unit.data.(transactionOutcomeReporter); ok {
			state = reporter.TransactionOutcome()
			state.Unit = index
			if state.State == "" {
				state.State = xhandler.TransactionUnknown
			}
		} else if unit.data == nil && unit.err == nil {
			state.State = xhandler.TransactionNone
		} else {
			unit.mu.Lock()
			state.Error = unit.completionErr
			unit.mu.Unlock()
		}
		result.Transactions = append(result.Transactions, state)
	}
	s.mu.Lock()
	s.completion = result
	s.mu.Unlock()
}

// completionOutcome is read only after the owning root has completed all units.
// Nested component seals do not publish a completion snapshot.
func (s *dataScope) completionOutcome() xhandler.Outcome {
	if s == nil {
		return xhandler.Outcome{}
	}
	root := s
	if s.root != nil {
		root = s.root
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	return root.completion.Clone()
}
