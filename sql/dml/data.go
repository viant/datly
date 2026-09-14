package dml

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/viant/datly/sql/sequencer"
	"github.com/viant/datly/sql/validation"
	"github.com/viant/sqlx/io/delete"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/io/update"
	"github.com/viant/sqlx/metadata/info"
	xhandler "github.com/viant/xdatly/handler"
)

var (
	ErrInvocationCompleted = errors.New("DML data invocation is completed")
	ErrInvocationFailed    = errors.New("DML data invocation failed")
	ErrComponentSealed     = errors.New("DML component frame is sealed")
	ErrBindingFlush        = errors.New("cannot flush a binding component while its ancestor is open")
)

type Data struct {
	mu           sync.Mutex
	executionMu  sync.Mutex
	db           *sql.DB
	tx           *sql.Tx
	externalTx   bool
	onCommit     func(context.Context)
	queue        []*dataOperation
	dialect      *info.Dialect
	invocation   bool
	completed    bool
	failed       error
	outcome      xhandler.TransactionOutcome
	outcomeReady bool
	root         *Data
	parent       *Data
	relation     string
	order        string
	open         bool
	markers      []componentMarker
	bindings     []*Data
	nextOp       uint64

	insertServices     map[string]*insert.Service
	updateServices     map[string]*update.Service
	deleteServices     map[string]*delete.Service
	sequencer          *sequencer.Service
	frameworkValidator *validation.Service
}

func NewData(db *sql.DB, opts ...Option) *Data {
	options := collectOptions(opts...)
	return &Data{
		db:             db,
		tx:             options.Tx,
		externalTx:     options.Tx != nil,
		onCommit:       options.OnCommit,
		insertServices: map[string]*insert.Service{},
		updateServices: map[string]*update.Service{},
		deleteServices: map[string]*delete.Service{},
		open:           true,
	}
}

func (d *Data) Allocate(ctx context.Context, tableName string, dest any, selector string) error {
	return d.sequence(ctx, func(s *sequencer.Service) error { return s.Allocate(ctx, tableName, dest, selector) })
}

// Reserve forwards pending supplied identities to the same scoped sequencer
// that will allocate IDs. Custom Sequencer implementations need only Allocate.
func (d *Data) Reserve(ctx context.Context, tableName string, dest any, selector string) error {
	return d.sequence(ctx, func(s *sequencer.Service) error { return s.Reserve(ctx, tableName, dest, selector) })
}

func (d *Data) sequence(ctx context.Context, run func(*sequencer.Service) error) error {
	db, err := d.database()
	if err != nil {
		return err
	}
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	invocation := owner.invocation
	owner.mu.Unlock()
	if !invocation {
		if owner.sequencer == nil {
			owner.sequencer = sequencer.New(db)
		}
		return run(owner.sequencer)
	}
	owner.mu.Lock()
	if owner.completed {
		owner.mu.Unlock()
		return ErrInvocationCompleted
	}
	if owner.failed != nil {
		failed := owner.failed
		owner.mu.Unlock()
		return errors.Join(ErrInvocationFailed, failed)
	}
	owner.mu.Unlock()
	tx, err := owner.transaction(ctx)
	if err != nil {
		return err
	}
	if owner.sequencer == nil {
		owner.sequencer = sequencer.New(db, tx)
	}
	err = run(owner.sequencer)
	if err != nil {
		owner.markFailed(err)
	}
	return err
}

// BeginInvocation switches Data from standalone auto-commit behavior to one
// root-owned transaction completed by Complete after output finalization.
func (d *Data) BeginInvocation() error {
	owner := d.owner()
	owner.executionMu.Lock()
	defer owner.executionMu.Unlock()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.completed {
		return ErrInvocationCompleted
	}
	if owner.invocation {
		return errors.New("DML data is already attached to an invocation")
	}
	owner.invocation = true
	// Standalone reservations used a different transaction strategy.
	owner.sequencer = nil
	return nil
}

func (d *Data) markFailed(err error) {
	if err == nil {
		return
	}
	owner := d.owner()
	owner.mu.Lock()
	if owner.failed == nil {
		owner.failed = err
	}
	owner.mu.Unlock()
}

func (d *Data) owner() *Data {
	if d != nil && d.root != nil {
		return d.root
	}
	return d
}
