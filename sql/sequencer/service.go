package sequencer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

type Service struct {
	db           *sql.DB
	tx           *sql.Tx
	mu           sync.Mutex
	reservations map[reservationKey]sink.Sequence
	pending      map[reservationKey]map[int64]bool
}

// Reserve records supplied values before the first allocation for a table. It
// does not allocate, change the destination, or advance a database sequence.
// Callers with original-presence evidence pass only originally supplied rows.
// Native read-only metadata unifies supplied-only aliases before allocation.
func (s *Service) Reserve(ctx context.Context, table string, dest any, selector string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	walker, err := NewWalker(dest, strings.Split(selector, "/"))
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
	key, err := s.nativeIdentity(ctx, inserter, record)
	if err != nil {
		return err
	}
	s.remember(key, cells)
	return nil
}

func (s *Service) nativeIdentity(ctx context.Context, inserter *insert.Service, record any) (reservationKey, error) {
	var options []option.Option
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
	key, err := s.nativeIdentity(ctx, inserter, record)
	if err != nil {
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
	strategy := dialect.PresetIDWithTransientTransaction
	if s.tx != nil {
		strategy = dialect.PresetIDWithMax
	}
	options := []option.Option{strategy}
	if s.tx != nil {
		options = append(options, s.tx)
	}
	values := make([]int64, 0, len(empty))
	count := len(empty)
	var previousRange *sink.Sequence
	for len(values) < len(empty) {
		if err := ctx.Err(); err != nil {
			return err
		}
		nextSeq, err := inserter.NextSequence(ctx, record, count, options...)
		if err != nil {
			return err
		}
		if nextSeq == nil || (reservationKey{nextSeq.Catalog, nextSeq.Schema, nextSeq.Name}) != key {
			return fmt.Errorf("native sequence identity changed before allocation")
		}
		if s.tx != nil {
			nextSeq, err = s.reserve(table, nextSeq, count)
		} else {
			err = s.validateRange(nextSeq, count)
		}
		if err != nil {
			return err
		}
		if previousRange != nil && nextSeq.Value <= previousRange.Value {
			return fmt.Errorf("native sequence reservation did not advance")
		}
		currentRange := *nextSeq
		previousRange = &currentRange
		// Consume only values inside this native reservation. Collisions require
		// another native range, not a jump past its exclusive high-water mark.
		value := nextSeq.MinValue(int64(count))
		for i := 0; i < count && len(values) < len(empty); i++ {
			if !pending[value] {
				values = append(values, value)
				pending[value] = true
			}
			if i+1 < count {
				value += nextSeq.IncrementBy
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
