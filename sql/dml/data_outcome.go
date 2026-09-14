package dml

import xhandler "github.com/viant/xdatly/handler"

// TransactionOutcome reports this existing Data owner's managed completion.
// It is not part of the public Data capability and exposes no transaction.
func (d *Data) TransactionOutcome() xhandler.TransactionOutcome {
	if d == nil {
		return xhandler.TransactionOutcome{State: xhandler.TransactionUnknown}
	}
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.outcomeReady {
		return owner.outcome
	}
	state := xhandler.TransactionNone
	if owner.externalTx {
		state = xhandler.TransactionCallerPending
	} else if owner.tx != nil {
		state = xhandler.TransactionUnknown
	}
	return xhandler.TransactionOutcome{State: state}
}

func (d *Data) recordOutcome(state xhandler.TransactionState, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.outcome = xhandler.TransactionOutcome{State: state, Error: err}
	d.outcomeReady = true
}
