package drainowner

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net"
	"reflect"
	"sync"

	"github.com/viant/bindly/locator"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/structology"
)

var ErrBindingGroup = errors.New("binding resolution group admission is invalid or closed")

type bindingFailure struct {
	cause       error
	group       *bindingGroupCell
	member      string
	terminal    bool
	observation uint64
}
type bindingGroupCell struct {
	identity *identity
	open     bool
	ctx      context.Context
	cancel   context.CancelFunc
	tickets  map[string]*bindingTicket
}
type bindingTicket struct {
	group           *bindingGroupCell
	member          string
	target          string
	inputs          []BindingGroupInput
	entered         bool
	targetValidated bool
	activityUsed    bool
	observation     uint64
	finished        bool
}

// BindingGroup and its context tickets are private single-invocation grants.
// They carry no mutation, preparation, drain or transaction authority.
type BindingGroup struct{ cell *bindingGroupCell }
type BindingGroupInput struct {
	Kind, In string
	Type     reflect.Type
	Adapted  bool
}
type BindingGroupMember struct {
	Path, Target string
	Inputs       []BindingGroupInput
}
type bindingGroupContextKey struct{}

func appendFailureLocked(ledger *activityLedger, cause error, group *bindingGroupCell, member string, terminal bool, observation uint64) {
	if cause == nil {
		return
	}
	if group == nil {
		terminal = true
	}
	ledger.failure = errors.Join(ledger.failure, cause)
	ledger.entries = append(ledger.entries, bindingFailure{cause: cause, group: group, member: member, terminal: terminal, observation: observation})
	if ledger.group != nil && ledger.group.open && (terminal || group != ledger.group) {
		ledger.group.cancel()
	}
}
func terminalBindingFailure(cause error) bool {
	if cause == nil {
		return false
	}
	if errors.Is(cause, ErrBindingGroup) || errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) || errors.Is(cause, sql.ErrTxDone) || errors.Is(cause, driver.ErrBadConn) || errors.Is(cause, mysql.ErrInvalidConn) {
		return true
	}
	var cleanup *sqlxread.CleanupError
	var network net.Error
	var panicError interface {
		Cause() any
		Stack() []byte
	}
	return errors.As(cause, &cleanup) || errors.As(cause, &network) || errors.As(cause, &panicError)
}
func groupFailureLocked(ledger *activityLedger, group *bindingGroupCell) error {
	for _, entry := range ledger.entries {
		if entry.terminal || entry.group != group {
			return ledger.failure
		}
	}
	return nil
}
func OpenBindingGroup(invocation *Invocation, members []BindingGroupMember) (BindingGroup, error) {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return BindingGroup{}, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.prefix != nil || externalPrefixLatched(invocation) {
		return BindingGroup{}, prefixDeniedLocked(ledger)
	}
	if !ledger.enrolled || ledger.closed || ledger.failure != nil || ledger.group != nil || len(members) == 0 {
		return BindingGroup{}, ErrBindingGroup
	}
	if ledger.transactionStarts != 0 {
		cause := fmt.Errorf("%w: transaction startup overlaps resolution group", ErrBindingGroup)
		appendFailureLocked(ledger, cause, nil, "", true, 0)
		return BindingGroup{}, cause
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := &bindingGroupCell{identity: invocation.identity, open: true, ctx: ctx, cancel: cancel, tickets: map[string]*bindingTicket{}}
	for _, member := range members {
		if member.Path == "" || group.tickets[member.Path] != nil {
			cancel()
			return BindingGroup{}, ErrBindingGroup
		}
		group.tickets[member.Path] = &bindingTicket{group: group, member: member.Path, target: member.Target, inputs: append([]BindingGroupInput(nil), member.Inputs...)}
	}
	ledger.group = group
	return BindingGroup{cell: group}, nil
}

// AdmitBindingGroupTransactionStart excludes group opening against an already
// serialized native startup. It grants no transaction or completion authority.
func AdmitBindingGroupTransactionStart(receiver any) (func(), error) {
	state, err := exactState(receiver)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	if state.claimed == nil {
		state.mu.Unlock()
		return func() {}, nil
	}
	ledger := &state.claimed.activities
	ledger.mu.Lock()
	if ledger.prefix != nil {
		err := prefixDeniedLocked(ledger)
		ledger.mu.Unlock()
		state.mu.Unlock()
		return nil, err
	}
	if ledger.group != nil && ledger.group.open {
		cause := fmt.Errorf("%w: transaction startup", ErrBindingGroup)
		appendFailureLocked(ledger, cause, nil, "", true, 0)
		ledger.mu.Unlock()
		state.mu.Unlock()
		return nil, cause
	}
	ledger.transactionStarts++
	ledger.mu.Unlock()
	state.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { ledger.mu.Lock(); ledger.transactionStarts--; ledger.mu.Unlock() }) }, nil
}
func (g BindingGroup) Enter(ctx context.Context, path string) (context.Context, error) {
	if g.cell == nil {
		return nil, ErrBindingGroup
	}
	ledger := &g.cell.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ticket := g.cell.tickets[path]
	if ledger.group != g.cell || !g.cell.open || ledger.closed || ticket == nil || ticket.entered {
		return nil, ErrBindingGroup
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := groupFailureLocked(ledger, g.cell); err != nil {
		return nil, err
	}
	ticket.entered = true
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(g.cell.ctx, cancel)
	context.AfterFunc(call, func() { stop() })
	return context.WithValue(call, bindingGroupContextKey{}, ticket), nil
}
func (g BindingGroup) Close() error {
	if g.cell == nil {
		return ErrBindingGroup
	}
	ledger := &g.cell.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.group != g.cell || !g.cell.open {
		return ErrBindingGroup
	}
	g.cell.open = false
	g.cell.cancel()
	ledger.group = nil
	for _, ticket := range g.cell.tickets {
		if ticket.activityUsed && !ticket.finished {
			err := fmt.Errorf("%w: group member %s", ErrActivityUnfinished, ticket.member)
			appendFailureLocked(ledger, err, nil, "", true, 0)
			return err
		}
	}
	return groupFailureLocked(ledger, g.cell)
}
func bindingContext(ctx context.Context) *bindingTicket {
	ticket, _ := ctx.Value(bindingGroupContextKey{}).(*bindingTicket)
	return ticket
}
func BindingGroupContext(ctx context.Context) bool { return bindingContext(ctx) != nil }
func BindingGroupOpen(invocation *Invocation) bool {
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return false
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	return ledger.group != nil && ledger.group.open
}
func BindingGroupAdmission(ctx context.Context, invocation *Invocation) error {
	ticket := bindingContext(ctx)
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ticket == nil || ticket.group.identity != invocation.identity || ledger.group != ticket.group || !ticket.group.open || !ticket.entered || ledger.closed {
		return ErrBindingGroup
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return groupFailureLocked(ledger, ticket.group)
}

// ValidateBindingGroupTarget runs at the actual runtime route/reader boundary.
// It cannot be enabled by a public context marker or by a claimed provider.
func ValidateBindingGroupTarget(ctx context.Context, target string, canonicalReader bool, inputs ...BindingGroupInput) error {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return nil
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.group != ticket.group || !ticket.group.open || !ticket.entered || ticket.target == "" || ticket.target != target || !canonicalReader || ticket.targetValidated || ticket.activityUsed {
		return ErrBindingGroup
	}
	if err := groupFailureLocked(ledger, ticket.group); err != nil {
		return err
	}
	for _, required := range ticket.inputs {
		matched := false
		for _, actual := range inputs {
			if actual.Kind == required.Kind && actual.In == required.In && actual.Type == required.Type && !actual.Adapted {
				matched = true
				break
			}
		}
		if !matched {
			appendFailureLocked(ledger, ErrBindingGroup, nil, "", true, 0)
			return ErrBindingGroup
		}
	}
	ticket.targetValidated = true
	return nil
}
func BindingGroupReadAdmission(ctx context.Context) (bool, error) {
	ticket := bindingContext(ctx)
	if ticket == nil {
		return false, nil
	}
	ledger := &ticket.group.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.group != ticket.group || !ticket.group.open || ledger.closed || !ticket.entered || !ticket.targetValidated {
		return false, ErrBindingGroup
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := groupFailureLocked(ledger, ticket.group); err != nil {
		return false, err
	}
	return true, nil
}
func AdmitBindingGroupActivity(ctx context.Context, invocation *Invocation) (Activity, error) {
	ticket := bindingContext(ctx)
	ledger, err := invocationLedger(invocation)
	if err != nil {
		return Activity{}, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ticket == nil || ticket.group.identity != invocation.identity || ledger.group != ticket.group || !ticket.group.open || !ticket.entered || !ticket.targetValidated || ticket.activityUsed || ledger.closed {
		return Activity{}, ErrBindingGroup
	}
	if err := ctx.Err(); err != nil {
		return Activity{}, err
	}
	if err := groupFailureLocked(ledger, ticket.group); err != nil {
		return Activity{}, err
	}
	ticket.activityUsed = true
	ledger.observation++
	ticket.observation = ledger.observation
	cell := &activityCell{identity: invocation.identity, group: ticket.group, member: ticket.member, observation: ticket.observation}
	if ledger.active == nil {
		ledger.active = map[*activityCell]struct{}{}
	}
	ledger.active[cell] = struct{}{}
	return Activity{cell: cell}, nil
}

// RecordBindingGroupFailure correlates an already retired native child. Only a
// scalar/observer failure without such a retirement appends a new observation.
func RecordBindingGroupFailure(g BindingGroup, path string, cause error, terminal bool) (uint64, error) {
	if g.cell == nil {
		return 0, ErrBindingGroup
	}
	ledger := &g.cell.identity.activities
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ticket := g.cell.tickets[path]
	if ledger.group != g.cell || !g.cell.open || ticket == nil {
		return 0, ErrBindingGroup
	}
	if ticket.activityUsed && ticket.finished {
		if terminal {
			appendFailureLocked(ledger, cause, nil, "", true, 0)
		}
		return ticket.observation, nil
	}
	if ticket.observation != 0 {
		return ticket.observation, ErrBindingGroup
	}
	if cause == nil {
		return 0, nil
	}
	ledger.observation++
	ticket.observation = ledger.observation
	appendFailureLocked(ledger, cause, g.cell, path, terminal || terminalBindingFailure(cause), ticket.observation)
	return ticket.observation, nil
}

// SealedBindingGroupProvider is installed only by the native runtime. Its
// unexported concrete identity cannot be supplied by application providers.
type bindingGroupProvider struct{ locator.Provider }

func (p bindingGroupProvider) Locate(source *structology.State) locator.Locator {
	return p.Provider.Locate(source)
}
func (p bindingGroupProvider) DefaultCacheable() bool {
	if policy, ok := p.Provider.(locator.CachePolicy); ok {
		return policy.DefaultCacheable()
	}
	return false
}
func SealBindingGroupProvider(provider locator.Provider) locator.Provider {
	return bindingGroupProvider{Provider: provider}
}
func IsBindingGroupProvider(provider locator.Provider) bool {
	_, ok := provider.(bindingGroupProvider)
	return ok
}
