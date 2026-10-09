package drainowner

import (
	"context"
	"errors"
	"strings"
)

var ErrPrefix = errors.New("protected database-prefix flush admission denied")

type prefixGrant struct {
	owner    *State
	frame    *Frame
	activity Activity
	target   any
	table    string
	selected map[any]JournalRecord
	bound    bool
	lifetime context.Context
	cancel   context.CancelFunc
	done     chan struct{}
}

type prefixReceipt struct {
	owner  *State
	frame  *Frame
	id     uint64
	permit *DrainPermit
}

func prefixDeniedLocked(ledger *activityLedger) error {
	appendFailureLocked(ledger, ErrPrefix, nil, "", true, 0)
	return ErrPrefix
}

// CallPrefix is engine authority for one explicitly configured, live caller.
// No application capability receives this handle or its private permit.
func (h Handle) CallPrefix(ctx context.Context, receiver any, invocation *Invocation, frame *Frame, activity Activity, target any, table string, lifetime context.Context) (err error) {
	s, err := h.attachmentState(receiver, invocation)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		FailProtected(invocation, err)
		return err
	}
	sqlContext, cancel := context.WithCancel(ctx)
	if lifetime == nil {
		cancel()
		FailProtected(invocation, ErrPrefix)
		return ErrPrefix
	}
	if err = lifetime.Err(); err != nil {
		cancel()
		FailProtected(invocation, err)
		return err
	}
	stopLifetime := context.AfterFunc(lifetime, cancel)
	defer stopLifetime()
	grant := &prefixGrant{owner: s, frame: frame, activity: activity, target: target, table: table, lifetime: lifetime, cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	ledger := &h.identity.activities
	ledger.mu.Lock()
	if s.claimed != h.identity || !s.attached || !s.attachmentReady || s.retired || s.pending != nil || s.operations[PrefixPreparation] == nil || table == "" || table != strings.ToLower(strings.TrimSpace(table)) || ledger.prefix != nil || len(ledger.drains) != 0 || ledger.transactionStarts != 0 {
		err = prefixDeniedLocked(ledger)
	} else {
		ledger.prefix = grant
		err = validatePrefixLocked(s, grant, ledger)
		if err != nil {
			ledger.prefix = nil
		}
	}
	ledger.mu.Unlock()
	s.mu.Unlock()
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		value := recover()
		s.mu.Lock()
		ledger.mu.Lock()
		if value != nil {
			err = errors.Join(err, ErrPrefix)
		}
		if err == nil {
			err = sqlContext.Err()
		}
		if err == nil {
			err = validatePrefixLocked(s, grant, ledger)
		}
		if err != nil {
			appendFailureLocked(ledger, err, nil, "", true, 0)
		}
		if ledger.prefix == grant {
			ledger.prefix = nil
		}
		ledger.mu.Unlock()
		s.mu.Unlock()
		cancel()
		close(grant.done)
		if value != nil {
			panic(value)
		}
	}()
	return h.call(sqlContext, receiver, invocation, PrefixPreparation, nil, grant)
}

// Caller holds State.mu then ledger.mu. Frame ancestry is journal-owned.
func validatePrefixLocked(s *State, grant *prefixGrant, ledger *activityLedger) error {
	if ledger.failure != nil {
		return ledger.failure
	}
	if grant == nil || grant.owner != s || ledger.prefix != grant || !ledger.enrolled || ledger.closed || grant.activity.cell == nil || grant.activity.cell.identity != s.claimed || grant.activity.cell.finished || grant.activity.cell.frame != grant.frame || ledger.group != nil && ledger.group.open {
		return prefixDeniedLocked(ledger)
	}
	if err := grant.lifetime.Err(); err != nil {
		appendFailureLocked(ledger, err, nil, "", true, 0)
		return err
	}
	if _, live := ledger.active[grant.activity.cell]; !live {
		return prefixDeniedLocked(ledger)
	}
	j := &s.claimed.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if grant.frame == nil || grant.frame.journal != j || !grant.frame.open || j.freezeStarted || j.failure != nil {
		return prefixDeniedLocked(ledger)
	}
	externalRoot := j.external[s]
	if externalRoot && (grant.frame != j.root || j.enabled || len(j.owners) != 1 || !j.owners[s] || !isolatedPrefixFrame(j.root, j.root)) {
		return prefixDeniedLocked(ledger)
	}
	// Prefix accounting activates ordered ownership even without buffered policy.
	for owner := range j.owners {
		if j.external[owner] && !(externalRoot && owner == s) || j.drained[owner] {
			return prefixDeniedLocked(ledger)
		}
	}
	j.prefixProtected = true
	for frame := grant.frame; frame != nil; frame = frame.parent {
		if frame.relation == "binding" {
			for parent := frame.parent; parent != nil; parent = parent.parent {
				if parent.open {
					return prefixDeniedLocked(ledger)
				}
			}
		}
	}
	for live := range ledger.active {
		if live == grant.activity.cell {
			continue
		}
		ancestor := false
		for frame := grant.frame.parent; frame != nil; frame = frame.parent {
			if live.frame == frame {
				ancestor = true
				break
			}
		}
		if !ancestor {
			return prefixDeniedLocked(ledger)
		}
	}
	if externalRoot {
		j.prefixExternalOwner = s
	}
	return nil
}

// Caller holds journal.mu. Closed read frames are permitted; their queued
// mutations and any live descendant would break the sole root boundary.
func isolatedPrefixFrame(frame, root *Frame) bool {
	if frame != root && frame.open {
		return false
	}
	for _, item := range frame.timeline {
		if item.entry != nil && item.entry.frame != root {
			return false
		}
		if item.child != nil && !isolatedPrefixFrame(item.child, root) {
			return false
		}
	}
	for _, child := range frame.bindings {
		if !isolatedPrefixFrame(child, root) {
			return false
		}
	}
	return true
}

// Caller holds ledger.mu; journal ownership comes only from native constructors.
func externalPrefixLatched(invocation *Invocation) bool {
	j := invocation.journal()
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.prefixExternalOwner != nil
}

// ExternalPrefix verifies the exact permit; it exposes no application authority.
func ExternalPrefix(receiver any, permit *DrainPermit) bool {
	s, err := exactState(receiver)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if permit == nil || permit.state != s || s.pending != permit || !permit.consumed || permit.prefix == nil {
		return false
	}
	j := &s.claimed.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.prefixExternalOwner == s && permit.prefix.frame == j.root && len(j.owners) == 1 && !j.enabled
}

// CheckPrefixMutation rejects mutation or callback reentry across every owner.
func CheckPrefixMutation(receiver any) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil {
		return nil
	}
	ledger := &s.claimed.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.prefix != nil {
		return prefixDeniedLocked(ledger)
	}
	return nil
}

func PrefixTarget(receiver any, permit *DrainPermit) (any, *Frame, string, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, nil, "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if permit == nil || permit.state != s || s.pending != permit || !permit.consumed || permit.prefix == nil {
		return nil, nil, "", ErrDrainPermit
	}
	return permit.prefix.target, permit.prefix.frame, permit.prefix.table, nil
}

// BindPrefixRecords retains the exact selection before any native effects.
func BindPrefixRecords(receiver any, permit *DrainPermit, records []JournalRecord) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if permit == nil || permit.state != s || s.pending != permit || !permit.consumed || permit.prefix == nil || permit.prefix.bound {
		return ErrDrainPermit
	}
	g := permit.prefix
	ledger := &s.claimed.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := validatePrefixLocked(s, g, ledger); err != nil {
		return err
	}
	j := &s.claimed.journal
	j.mu.Lock()
	var entries []*journalEntry
	collectLivePrefixEntries(j.root, &entries)
	j.mu.Unlock()
	journalRecords := make(map[any]*journalEntry, len(entries))
	for _, entry := range entries {
		journalRecords[entry.record] = entry
	}
	g.selected = map[any]JournalRecord{}
	for _, record := range records {
		if record.Record == nil || record.ID == 0 || record.Frame == nil || record.Frame.journal != &s.claimed.journal || record.Executed {
			return prefixDeniedLocked(ledger)
		}
		entry := journalRecords[record.Record]
		if entry == nil || entry.owner != s || entry.frame != record.Frame {
			return prefixDeniedLocked(ledger)
		}
		if _, duplicate := g.selected[record.Record]; duplicate {
			return prefixDeniedLocked(ledger)
		}
		g.selected[record.Record] = record
	}
	g.bound = true
	return nil
}

// RecordPrefixStep attests only a successfully returned native plan step.
func RecordPrefixStep(receiver any, permit *DrainPermit, records []JournalRecord) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if permit == nil || permit.state != s || s.pending != permit || !permit.consumed || permit.prefix == nil || !permit.prefix.bound {
		return ErrDrainPermit
	}
	j := &s.claimed.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.prefixReceipts == nil {
		j.prefixReceipts = map[any]prefixReceipt{}
	}
	for _, record := range records {
		selected, ok := permit.prefix.selected[record.Record]
		if !ok || selected.Frame != record.Frame || selected.ID != record.ID || !record.Executed {
			return ErrJournal
		}
		if _, duplicate := j.prefixReceipts[record.Record]; duplicate {
			return ErrJournal
		}
	}
	if len(records) != 0 {
		j.prefixExecuted = true
	}
	for _, record := range records {
		j.prefixReceipts[record.Record] = prefixReceipt{owner: s, frame: record.Frame, id: record.ID, permit: permit}
	}
	return nil
}

// JoinPrefixCompletion is internal engine closure authority. Admission must
// already be closed. It joins native unwinding only, never the active handler.
func JoinPrefixCompletion(invocation *Invocation) error {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	if !ledger.closed {
		err = prefixDeniedLocked(ledger)
		ledger.mu.Unlock()
		return err
	}
	grant := ledger.prefix
	ledger.mu.Unlock()
	if grant != nil {
		grant.cancel()
		<-grant.done
	}
	return ProtectedFailure(invocation)
}

// PrefixTransactionContext keeps root transaction lifetime separate from the
// cancellable SQL operation; successful explicit flush never ends that lifetime.
func PrefixTransactionContext(receiver any, permit *DrainPermit) (context.Context, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if permit == nil || permit.state != s || s.pending != permit || !permit.consumed || permit.prefix == nil {
		return nil, ErrDrainPermit
	}
	return permit.prefix.lifetime, nil
}

// Selection observes live frame records under journal.mu; it does not seal
// frames or borrow the completion-only flattening admission.
func collectLivePrefixEntries(frame *Frame, entries *[]*journalEntry) {
	for _, item := range frame.timeline {
		if item.child != nil {
			collectLivePrefixEntries(item.child, entries)
		} else {
			*entries = append(*entries, item.entry)
		}
	}
	for _, child := range frame.bindings {
		collectLivePrefixEntries(child, entries)
	}
}

// BeginNativeEffect records serialized native work before execution locks.
// It grants no flush or completion permission. Prefix admission cannot enter
// from a sequence/validation/SQL callback holding any invocation owner's lock.
func BeginNativeEffect(receiver any) (*DrainRecord, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed == nil {
		return nil, nil
	}
	ledger := &s.claimed.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.prefix != nil {
		return nil, prefixDeniedLocked(ledger)
	}
	return addDrainLocked(s, s.claimed), nil
}
