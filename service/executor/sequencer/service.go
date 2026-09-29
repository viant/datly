package sequencer

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"github.com/viant/sqlx/option"
	"strings"
)

type Service struct {
	db  *sql.DB
	tx  *sql.Tx
	ctx context.Context
}

func (s *Service) Next(table string, any interface{}, selector string) error {
	err := s.next(table, any, selector)
	if err != nil {
		return fmt.Errorf("failed to allocate %v sequence due to: %w", table, err)
	}
	return nil
}

func (s *Service) next(table string, any interface{}, selector string) error {
	parts := strings.Split(selector, "/")
	aWalker, err := NewWalker(any, parts)
	if err != nil {
		return err
	}
	emptyRecordCount, err := aWalker.CountEmpty(any)
	if err != nil || emptyRecordCount == 0 {
		return err
	}
	record, err := aWalker.EmptyLeaf(any)
	if err != nil {
		return err
	}
	if record == nil {
		return nil
	}
	inserter, err := insert.New(s.ctx, s.db, table)
	if err != nil {
		return err
	}
	options := []option.Option{dialect.PresetIDWithTransientTransaction}
	if s.tx != nil {
		options = append(options, s.tx)
	}
	nextSeq, err := inserter.NextSequence(s.ctx, record, emptyRecordCount, options...)
	if err != nil {
		return err
	}
	seq := &Sequence{Value: nextSeq.MinValue(int64(emptyRecordCount)), IncrementBy: nextSeq.IncrementBy}
	err = aWalker.Allocate(any, seq)

	return err
}

func New(ctx context.Context, db *sql.DB, tx ...*sql.Tx) *Service {
	ret := &Service{db: db, ctx: ctx}
	if len(tx) > 0 {
		ret.tx = tx[0]
	}
	return ret
}
