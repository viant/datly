package sequencer

import (
	"context"
	"database/sql"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/metadata/info/dialect"
	"strings"
)

type Service struct {
	db  *sql.DB
	ctx context.Context
}

func (s *Service) Next(table string, any interface{}, selector string) error {
	parts := strings.Split(selector, "/")
	aWalker, err := NewWalker(any, parts)
	if err != nil {
		return err
	}
	emptyRecordCount, err := aWalker.CountEmpty(any)
	if err != nil || emptyRecordCount == 0 {
		return err
	}
	record, err := aWalker.Leaf(any)
	if err != nil {
		return err
	}
	inserter, err := insert.New(s.ctx, s.db, table)
	if err != nil {
		return err
	}
	nextSeq, err := inserter.NextSequence(s.ctx, record, emptyRecordCount, dialect.PresetIDWithTransientTransaction)
	if err != nil {
		return err
	}
	seq := &Sequence{Value: nextSeq.MinValue(int64(emptyRecordCount)), IncrementBy: nextSeq.IncrementBy}
	err = aWalker.Allocate(any, seq)

	return err
}

func New(ctx context.Context, db *sql.DB) *Service {
	return &Service{db: db, ctx: ctx}
}
