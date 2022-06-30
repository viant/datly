package reader

import (
	"context"
	"database/sql"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"sync"
	"time"
)

//Service represents reader service
type Service struct {
	sqlBuilder *Builder
	Resource   *view.Resource
}

//Read select view from database based on View and assign it to dest. ParentDest has to be pointer.
func (s *Service) Read(ctx context.Context, session *Session) error {
	var err error
	start := time.Now()
	onFinish := session.View.Counter.Begin(start)
	defer s.afterRead(session, &start, err, onFinish)

	if err = session.Init(); err != nil {
		return err
	}

	wg := sync.WaitGroup{}

	collector := session.View.Collector(session.Dest, session.View.MatchStrategy.SupportsParallel())
	errors := shared.NewErrors(0)
	s.readAll(ctx, session, collector, &wg, errors)
	wg.Wait()
	err = errors.Error()
	if err != nil {
		return err
	}
	collector.MergeData()

	if err = errors.Error(); err != nil {
		return err
	}

	if dest, ok := session.Dest.(*interface{}); ok {
		*dest = collector.Dest()
	}
	return nil
}

func (s *Service) afterRead(session *Session, start *time.Time, err error, onFinish counter.OnDone) {
	end := time.Now()
	session.View.Logger.ReadTime(session.View.Name, start, &end, err)
	if err != nil {
		session.View.Counter.IncrementValue(Error)
	} else {
		session.View.Counter.IncrementValue(Success)
	}
	onFinish(end)
}

func (s *Service) readAll(ctx context.Context, session *Session, collector *view.Collector, wg *sync.WaitGroup, errorCollector *shared.Errors) {
	var collectorFetchEmitted bool
	defer s.afterReadAll(collectorFetchEmitted, collector)

	aView := collector.View()
	selector := session.Selectors.Lookup(aView)
	collectorChildren := collector.Relations(selector)
	wg.Add(len(collectorChildren))

	relationGroup := sync.WaitGroup{}
	if !collector.SupportsParallel() {
		relationGroup.Add(len(collectorChildren))
	}
	for i := range collectorChildren {
		go func(i int) {
			defer s.afterRelationCompleted(wg, collector, &relationGroup)
			s.readAll(ctx, session, collectorChildren[i], wg, errorCollector)
		}(i)
	}

	collector.WaitIfNeeded()
	batchData := s.batchData(collector)
	if batchData.ColumnName != "" && len(batchData.Values) == 0 {
		return
	}

	db, err := aView.Db()
	if err != nil {
		errorCollector.Append(err)
		return
	}

	session.View.Counter.IncrementValue(Pending)
	defer session.View.Counter.DecrementValue(Pending)
	err = s.exhaustRead(ctx, aView, selector, batchData, db, collector, session)
	if err != nil {
		errorCollector.Append(err)
	}
	if collector.SupportsParallel() {
		return
	}

	collectorFetchEmitted = true
	collector.Fetched()

	relationGroup.Wait()
	ptr, xslice := collector.Slice()
	for i := 0; i < xslice.Len(ptr); i++ {
		if actual, ok := xslice.ValuePointerAt(ptr, i).(OnRelationer); ok {
			actual.OnRelation(ctx)
			continue
		}

		break
	}
}

func (s *Service) afterRelationCompleted(wg *sync.WaitGroup, collector *view.Collector, relationGroup *sync.WaitGroup) {
	wg.Done()
	if collector.SupportsParallel() {
		return
	}
	relationGroup.Done()
}

func (s *Service) afterReadAll(collectorFetchEmitted bool, collector *view.Collector) {
	if collectorFetchEmitted {
		return
	}
	collector.Fetched()
}

func (s *Service) batchData(collector *view.Collector) *BatchData {
	batchData := &BatchData{}

	batchData.Values, batchData.ColumnName = collector.ParentPlaceholders()
	batchData.ParentReadSize = len(batchData.Values)

	return batchData
}

func (s *Service) exhaustRead(ctx context.Context, view *view.View, selector *view.Selector, batchData *BatchData, db *sql.DB, collector *view.Collector, session *Session) error {
	batchData.ValuesBatch, batchData.Parent = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
	visitor := collector.Visitor()

	for {
		SQL, args, err := s.sqlBuilder.Build(view, selector, batchData, collector.Relation(), session.Parent)
		if err != nil {
			return err
		}

		err = s.query(ctx, view, db, SQL, collector, args, visitor)
		if err != nil {
			return err
		}

		if batchData.Parent == batchData.ParentReadSize {
			break
		}

		var nextParents int
		batchData.ValuesBatch, nextParents = sliceWithLimit(batchData.Values, batchData.Parent, batchData.Parent+view.Batch.Parent)
		batchData.Parent += nextParents
	}
	return nil
}

func (s *Service) query(ctx context.Context, view *view.View, db *sql.DB, SQL string, collector *view.Collector, args []interface{}, visitor view.Visitor) error {
	begin := time.Now()
	reader, err := read.New(ctx, db, SQL, collector.NewItem(), io.Resolve(collector.Resolve))
	if err != nil {
		return err
	}

	defer reader.Stmt().Close()
	readData := 0
	err = reader.QueryAll(ctx, func(row interface{}) error {
		readData++
		if fetcher, ok := row.(OnFetcher); ok {
			if err = fetcher.OnFetch(ctx); err != nil {
				return err
			}
		}
		return visitor(row)
	}, args...)
	end := time.Now()
	view.Logger.ReadingData(end.Sub(begin), SQL, readData, args, err)
	if err != nil {
		return err
	}

	return nil
}

//New creates Service instance
func New() *Service {
	return &Service{
		sqlBuilder: NewBuilder(),
		Resource:   view.EmptyResource(),
	}
}
