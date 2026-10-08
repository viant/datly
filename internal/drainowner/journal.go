package drainowner

import (
	"errors"
	"sort"
	"sync"
)

var ErrOrderedComposition = errors.New("buffered cross-owner composition requires undrained locally owned native transactions")
var ErrJournal = errors.New("native invocation journal association or cursor is invalid")

// Journal locks protect bookkeeping only. Native lock order is executionMu,
// establishment gate, owner.mu, State.mu, journal.mu. SQL never holds journal.mu.
// Frame creation takes only journal.mu; closure joins engine admission first.
type Frame struct {
	journal  *journal
	parent   *Frame
	open     bool
	order    string
	timeline []journalItem
	bindings []*Frame
}
type journalItem struct {
	child *Frame
	entry *journalEntry
}
type journalEntry struct {
	owner  *State
	frame  *Frame
	record any
}
type journalRun struct {
	owner   *State
	entries []*journalEntry
	index   int
}
type journal struct {
	mu            sync.Mutex
	establish     sync.Mutex
	root          *Frame
	owners        map[*State]bool
	external      map[*State]bool
	drained       map[*State]bool
	enabled       bool
	frozen        bool
	freezeStarted bool
	runs          []*journalRun
	cursor        int
	failure       error
	txOrder       []*State
	txIDs         map[*State]any
}

// NativeJournal callbacks are bound once in the Data constructor. They do not
// grant handlers execution authority; frame association checks actual ownership.
type JournalRecord struct {
	Record any
	Frame  *Frame
	ID     uint64
}

type NativeJournal struct {
	Bind     func(any, *Frame) error
	Snapshot func() []JournalRecord
	External bool
}

func RegisterJournal(receiver any, native NativeJournal) {
	s, err := exactState(receiver)
	if err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimed != nil || s.journalNative != nil {
		panic(ErrJournal)
	}
	s.journalNative = &native
}
func (i *Invocation) journal() *journal {
	if i == nil || i.identity == nil {
		return nil
	}
	return &i.identity.journal
}
func (i *Invocation) RootFrame() *Frame {
	j := i.journal()
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root == nil {
		j.root = &Frame{journal: j, open: true}
		j.owners = map[*State]bool{}
		j.external = map[*State]bool{}
		if j.drained == nil {
			j.drained = map[*State]bool{}
		}
		if j.txIDs == nil {
			j.txIDs = map[*State]any{}
		}
	}
	return j.root
}
func (i *Invocation) ChildFrame(parent *Frame, relation, order string) (*Frame, error) {
	i.RootFrame()
	j := i.journal()
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.freezeStarted || parent == nil || parent.journal != j {
		return nil, ErrJournal
	}
	for !parent.open && parent.parent != nil {
		parent = parent.parent
	}
	f := &Frame{journal: j, parent: parent, open: true, order: order}
	if relation == "binding" {
		parent.bindings = append(parent.bindings, f)
		sort.SliceStable(parent.bindings, func(a, b int) bool { return parent.bindings[a].order < parent.bindings[b].order })
	} else {
		parent.timeline = append(parent.timeline, journalItem{child: f})
	}
	return f, nil
}
func SealFrame(f *Frame) {
	if f == nil {
		return
	}
	f.journal.mu.Lock()
	f.open = false
	f.journal.mu.Unlock()
}
func (h Handle) BindFrame(receiver, component any, i *Invocation, f *Frame) error {
	s, err := h.attachmentState(receiver, i)
	if err != nil {
		return err
	}
	s.mu.Lock()
	ready := s.attached && s.attachmentReady && !s.retired
	native := s.journalNative
	drained := s.historicalDrain
	s.mu.Unlock()
	if !ready || native == nil || native.Bind == nil || f == nil || f.journal != i.journal() {
		return ErrJournal
	}
	j := i.journal()
	j.mu.Lock()
	if j.freezeStarted {
		j.mu.Unlock()
		return ErrJournal
	}
	j.owners[s] = true
	j.external[s] = native.External
	j.drained[s] = j.drained[s] || drained
	err = j.admissionLocked()
	j.mu.Unlock()
	if err != nil {
		FailProtected(i, err)
		return err
	}
	return native.Bind(component, f)
}
func (j *journal) admissionLocked() error {
	if !j.enabled || len(j.owners) < 2 {
		return nil
	}
	for s := range j.owners {
		if j.external[s] || j.drained[s] {
			return ErrOrderedComposition
		}
	}
	return nil
}
func (i *Invocation) EnableJournal() error {
	i.RootFrame()
	j := i.journal()
	j.mu.Lock()
	j.enabled = true
	err := j.admissionLocked()
	j.mu.Unlock()
	if err != nil {
		FailProtected(i, err)
	}
	return err
}

// AppendJournal runs under owner.mu. A failed association exposes no local
// append. Both projections become visible before mutation admission is released.
func AppendJournal(receiver any, f *Frame, record any) error {
	s, err := exactState(receiver)
	if err != nil {
		return err
	}
	s.mu.Lock()
	issuer := s.claimed
	attached := s.attached
	s.mu.Unlock()
	if issuer == nil {
		return nil
	}
	j := &issuer.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.root == nil || !j.owners[s] {
		return nil
	}
	if f == nil || f.journal != j || j.freezeStarted || !f.open || !attached || !j.owners[s] {
		return ErrJournal
	}
	f.timeline = append(f.timeline, journalItem{entry: &journalEntry{owner: s, frame: f, record: record}})
	return nil
}
func flattenFrame(f *Frame, out *[]*journalEntry) error {
	if f != f.journal.root && f.open {
		return ErrJournal
	}
	for _, item := range f.timeline {
		if item.child != nil {
			if err := flattenFrame(item.child, out); err != nil {
				return err
			}
		} else {
			*out = append(*out, item.entry)
		}
	}
	for _, child := range f.bindings {
		if err := flattenFrame(child, out); err != nil {
			return err
		}
	}
	return nil
}

// Freeze validates the exact native projections outside bookkeeping locks.
func (i *Invocation) FreezeJournal() (ordered bool, retErr error) {
	j := i.journal()
	if j == nil {
		return false, nil
	}
	defer func() {
		if retErr != nil {
			j.mu.Lock()
			if j.failure == nil {
				j.failure = retErr
			}
			j.mu.Unlock()
			FailProtected(i, retErr)
		}
	}()
	j.mu.Lock()
	if !j.enabled || len(j.owners) < 2 {
		j.mu.Unlock()
		return false, nil
	}
	if j.frozen {
		failure := j.failure
		j.mu.Unlock()
		return true, failure
	}
	if j.freezeStarted {
		failure := j.failure
		if failure == nil {
			failure = ErrJournal
		}
		j.mu.Unlock()
		return true, failure
	}
	if err := j.admissionLocked(); err != nil {
		j.mu.Unlock()
		return true, err
	}
	var entries []*journalEntry
	err := flattenFrame(j.root, &entries)
	owners := make([]*State, 0, len(j.owners))
	for s := range j.owners {
		owners = append(owners, s)
	}
	j.freezeStarted = true
	j.mu.Unlock()
	if err != nil {
		return true, err
	}
	seen := map[any]*journalEntry{}
	for _, e := range entries {
		if e == nil || seen[e.record] != nil {
			return true, ErrJournal
		}
		seen[e.record] = e
	}
	for _, s := range owners {
		s.mu.Lock()
		valid := s.claimed == i.identity && s.attached && s.attachmentReady && !s.retired
		native := s.journalNative
		s.mu.Unlock()
		if !valid || native == nil || native.Snapshot == nil {
			return true, ErrJournal
		}
		records := native.Snapshot()
		ids := map[uint64]bool{}
		for _, r := range records {
			e := seen[r.Record]
			if e == nil || e.owner != s || e.frame != r.Frame || r.ID == 0 || ids[r.ID] {
				return true, ErrJournal
			}
			ids[r.ID] = true
			delete(seen, r.Record)
		}
	}
	if len(seen) != 0 {
		return true, ErrJournal
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, e := range entries {
		if len(j.runs) == 0 || j.runs[len(j.runs)-1].owner != e.owner {
			j.runs = append(j.runs, &journalRun{owner: e.owner, index: len(j.runs)})
		}
		run := j.runs[len(j.runs)-1]
		run.entries = append(run.entries, e)
	}
	if j.failure != nil {
		return true, j.failure
	}
	j.frozen = true
	return true, nil
}
func (i *Invocation) NextJournalOwner() any {
	j := i.journal()
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cursor == len(j.runs) {
		return nil
	}
	return j.runs[j.cursor].owner.receiver
}
func (j *journal) permitRun(s *State, op Operation) (*journalRun, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failure != nil && op != Abort {
		return nil, j.failure
	}
	if op == Abort {
		return nil, nil
	}
	if !j.freezeStarted {
		return nil, nil
	}
	if !j.frozen {
		return nil, ErrJournal
	}
	if op == Completion {
		if j.cursor != len(j.runs) {
			return nil, ErrJournal
		}
		return nil, nil
	}
	if op != LocalPreparation && op != AllPreparation {
		return nil, ErrJournal
	}
	if j.cursor == len(j.runs) {
		return nil, nil
	}
	run := j.runs[j.cursor]
	if run.owner != s {
		return nil, ErrJournal
	}
	return run, nil
}
func RunRecords(receiver any, p *DrainPermit) ([]any, error) {
	s, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p == nil || p.state != s || s.pending != p || !p.consumed {
		return nil, ErrDrainPermit
	}
	if p.run == nil {
		return nil, nil
	}
	j := &p.identity.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if !j.frozen || j.cursor != p.run.index || j.runs[j.cursor] != p.run {
		return nil, ErrJournal
	}
	result := make([]any, len(p.run.entries))
	for n, e := range p.run.entries {
		result[n] = e.record
	}
	return result, nil
}
func finishJournalRun(p *DrainPermit, success bool) error {
	if p.run == nil || !success {
		return nil
	}
	j := &p.identity.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cursor != p.run.index || j.runs[j.cursor] != p.run {
		return ErrJournal
	}
	j.cursor++
	return nil
}
func NoteJournalDrain(receiver any) {
	s, err := exactState(receiver)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.historicalDrain = true
	issuer := s.claimed
	s.mu.Unlock()
	if issuer == nil {
		return
	}
	j := &issuer.journal
	j.mu.Lock()
	if j.drained == nil {
		j.drained = map[*State]bool{}
	}
	j.drained[s] = true
	j.mu.Unlock()
}

// The gate serializes successful Begin publication, not frame admissions. It is
// never taken while holding owner.mu, and callbacks never hold journal.mu.
func EstablishmentGate(receiver any) func(any) {
	s, err := exactState(receiver)
	if err != nil {
		return func(any) {}
	}
	s.mu.Lock()
	issuer := s.claimed
	s.mu.Unlock()
	if issuer == nil {
		return func(any) {}
	}
	j := &issuer.journal
	j.establish.Lock()
	return func(tx any) {
		if tx != nil {
			j.mu.Lock()
			if j.txIDs == nil {
				j.txIDs = map[*State]any{}
			}
			if _, exists := j.txIDs[s]; !exists {
				j.txIDs[s] = tx
				j.txOrder = append(j.txOrder, s)
			}
			j.mu.Unlock()
		}
		j.establish.Unlock()
	}
}
func (i *Invocation) TransactionOwners() []any {
	j := i.journal()
	if j == nil {
		return nil
	}
	j.establish.Lock()
	defer j.establish.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	result := make([]any, len(j.txOrder))
	for n, s := range j.txOrder {
		result[n] = s.receiver
	}
	return result
}

func OrderedJournal(receiver any) bool {
	s, err := exactState(receiver)
	if err != nil {
		return false
	}
	s.mu.Lock()
	issuer := s.claimed
	s.mu.Unlock()
	if issuer == nil {
		return false
	}
	j := &issuer.journal
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.freezeStarted && j.enabled && len(j.owners) > 1
}

func failJournalRun(p *DrainPermit, cause error) {
	if p == nil || p.run == nil || cause == nil {
		return
	}
	j := &p.identity.journal
	j.mu.Lock()
	if j.failure == nil {
		j.failure = cause
	}
	j.mu.Unlock()
}
