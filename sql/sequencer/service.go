package sequencer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
)

type Service struct {
	db       *sql.DB
	tx       *sql.Tx
	mu       sync.Mutex
	pending  map[reservationKey]map[int64]bool
	strategy dialect.PresetIDStrategy
}

// Reserve records supplied values before the first allocation for a table. It
// does not allocate, change the destination, or advance a database sequence.
// Callers with original-presence evidence pass only originally supplied rows.
// Native metadata unifies supplied-only aliases before allocation. A supplied
// transaction acquires native write intent before reading sequence identity.
func (s *Service) Reserve(ctx context.Context, table string, dest any, selector string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	parts := strings.Split(selector, "/")
	walker, err := NewWalker(dest, parts)
	if err != nil {
		return err
	}
	cells, err := walker.cells(ctx, walker.root, dest)
	if err != nil {
		return err
	}
	if len(cells) == 0 {
		return nil
	}
	record, err := walker.Leaf(dest)
	if err != nil {
		return err
	}
	inserter, err := insert.New(ctx, s.db, table)
	if err != nil {
		return err
	}
	key, err := s.nativeIdentity(ctx, inserter, record, parts[len(parts)-1])
	if err != nil {
		return err
	}
	s.remember(key, cells)
	return nil
}

func (s *Service) nativeIdentity(ctx context.Context, inserter *insert.Service, record any, field string) (reservationKey, error) {
	options := s.options(field)
	if s.tx != nil {
		options = append(options, s.tx)
	}
	info, err := inserter.SequenceInfo(ctx, record, options...)
	if err != nil {
		return reservationKey{}, err
	}
	if info == nil || info.Name == "" {
		return reservationKey{}, fmt.Errorf("native sequence identity is unresolved")
	}
	return reservationKey{info.Catalog, info.Schema, info.Name}, nil
}

func (s *Service) remember(key reservationKey, cells []*integerCell) map[int64]bool {
	if s.pending == nil {
		s.pending = make(map[reservationKey]map[int64]bool)
	}
	values := s.pending[key]
	if values == nil {
		// Zero is the Allocate API's empty sentinel, never an allocated ID.
		values = map[int64]bool{0: true}
		s.pending[key] = values
	}
	for _, cell := range cells {
		if value, present := cell.integer(); present {
			values[value] = true
		}
	}
	return values
}

func (s *Service) Allocate(ctx context.Context, table string, dest any, selector string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.allocate(ctx, table, dest, selector)
	if err != nil {
		return fmt.Errorf("failed to allocate %v sequence due to: %w", table, err)
	}
	return nil
}

func (s *Service) allocate(ctx context.Context, table string, dest any, selector string) error {
	parts := strings.Split(selector, "/")
	aWalker, err := NewWalker(dest, parts)
	if err != nil {
		return err
	}
	cells, err := aWalker.cells(ctx, aWalker.root, dest)
	if err != nil {
		return err
	}
	empty := make([]*integerCell, 0, len(cells))
	locations := make(map[uintptr]bool)
	for _, cell := range cells {
		if zero, err := cell.isZero(); err != nil {
			return err
		} else if zero {
			location := cell.location()
			if locations[location] {
				return fmt.Errorf("empty sequence fields share a writable identity holder")
			}
			locations[location] = true
			empty = append(empty, cell)
		}
	}
	if len(cells) == 0 {
		return nil
	}
	record, err := aWalker.Leaf(dest)
	if err != nil {
		return err
	}
	if record == nil {
		return nil
	}
	inserter, err := insert.New(ctx, s.db, table)
	if err != nil {
		return err
	}
	key, err := s.nativeIdentity(ctx, inserter, record, parts[len(parts)-1])
	if err != nil {
		// A fully supplied identity does not require allocation. Products with
		// native reservation still register its value for later batches.
		if len(empty) == 0 && errors.Is(err, metadata.ErrSequenceReservationUnsupported) {
			return nil
		}
		return err
	}
	pending := s.remember(key, cells)
	if len(empty) == 0 {
		return nil
	}
	record, err = aWalker.EmptyLeaf(dest)
	if err != nil {
		return err
	}
	options := s.options(parts[len(parts)-1])
	if s.tx != nil {
		options = append(options, s.tx)
	}
	values := make([]int64, 0, len(empty))
	count := len(empty)

	for len(values) < len(empty) {
		if err := ctx.Err(); err != nil {
			return err
		}
		reservation, err := inserter.ReserveSequence(ctx, record, count, options...)
		if err != nil {
			return err
		}
		if reservation == nil || (reservationKey{reservation.Sequence.Catalog, reservation.Sequence.Schema, reservation.Sequence.Name}) != key {
			return fmt.Errorf("native sequence identity changed before allocation")
		}
		if err := reservation.Validate(count); err != nil {
			return err
		}
		for _, value := range reservation.Values {
			if !pending[value] {
				values = append(values, value)
				pending[value] = true
			}
		}
		count = len(empty) - len(values)
	}
	for i, cell := range empty {
		if err := cell.check(values[i]); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for i, cell := range empty {
		if err := cell.set(values[i]); err != nil {
			return err
		}
	}
	return nil
}

func New(db *sql.DB, tx ...*sql.Tx) *Service {
	ret := &Service{db: db}
	if len(tx) > 0 {
		ret.tx = tx[0]
	}
	return ret
}

// WithStrategy explicitly selects a native strategy. Empty means the product
// default; Datly never substitutes a strategy based on transaction presence.
// Configure before the service is first used.
func (s *Service) WithStrategy(strategy dialect.PresetIDStrategy) *Service {
	s.strategy = strategy
	return s
}

func (s *Service) options(field string) []option.Option {
	result := []option.Option{option.SequenceField(field), option.SequenceReservationIntent(true)}
	if s.strategy != "" && s.strategy != dialect.PresetIDStrategyUndefined {
		result = append(result, s.strategy)
	}
	return result
}
