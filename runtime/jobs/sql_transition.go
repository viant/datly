package jobs

import (
	"context"
	"fmt"
	xasync "github.com/viant/xdatly/async"
	"time"
)

// Completion uses native typed updates. The one conditional claim statement
// below owns job lifecycle predicates that native identity updates cannot express.
type completionUpdate struct {
	Status         xasync.Status
	EndTime        *time.Time
	ExpiryTime     *time.Time
	RunTimeInMcs   int
	Error          *string
	Metrics        string
	SQLQuery       string
	CacheKey       *string
	CacheSet       *string
	CacheNamespace *string
	ID             string        `sqlx:"name=ID,primaryKey=true"`
	ExpectedStatus xasync.Status `sqlx:"name=Status,primaryKey=true"`
	Started        *time.Time    `sqlx:"name=StartTime,primaryKey=true"`
}

func (s *SQLStore) claim(ctx context.Context, r *Record, now time.Time) error {
	if !r.active(now) {
		return ErrTransition
	}
	wait := int(now.Sub(r.CreationTime).Microseconds())
	// SQLX's identity updater cannot express nullable deactivation and expiry
	// inequalities. Keep this bounded CAS in the job policy, with native dialect
	// placeholders; row mapping and ordinary writes remain SQLX-owned.
	query := s.dialect.EnsurePlaceholders("UPDATE " + s.table + " SET Status = ?, StartTime = ?, WaitTimeInMcs = ? WHERE ID = ? AND Status = ? AND (Deactivated = ? OR Deactivated IS NULL) AND (ExpiryTime IS NULL OR ExpiryTime > ?)")
	result, err := s.db.ExecContext(ctx, query, xasync.StatusRunning, now, wait, r.ID, xasync.StatusPending, false, now)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err := s.changed(count, err); err != nil {
		return err
	}
	r.Status, r.StartTime, r.WaitTimeInMcs = xasync.StatusRunning, &now, wait

	return nil
}
func (s *SQLStore) complete(ctx context.Context, r *Record) error {
	count, err := s.completer.Exec(ctx, &completionUpdate{Status: r.Status, EndTime: r.EndTime, ExpiryTime: r.ExpiryTime, RunTimeInMcs: r.RunTimeInMcs, Error: r.Error, Metrics: r.Metrics, SQLQuery: r.SQLQuery, CacheKey: r.CacheKey, CacheSet: r.CacheSet, CacheNamespace: r.CacheNamespace, ID: r.ID, ExpectedStatus: xasync.StatusRunning, Started: r.StartTime})
	return s.changed(count, err)
}

type deactivateUpdate struct {
	Deactivated    bool
	ID             string        `sqlx:"name=ID,primaryKey=true"`
	ExpectedStatus xasync.Status `sqlx:"name=Status,primaryKey=true"`
}

func (s *SQLStore) Expire(ctx context.Context, now time.Time) (int64, error) {
	records, err := s.read(ctx, "WHERE (Deactivated = ? OR Deactivated IS NULL) AND ExpiryTime <= ? AND Status <> ?", false, now, xasync.StatusRunning)
	if err != nil {
		return 0, err
	}
	var count int64
	for _, r := range records {
		changed, err := s.deactivator.Exec(ctx, &deactivateUpdate{Deactivated: true, ID: r.ID, ExpectedStatus: r.Status})
		if err != nil {
			return count, err
		}
		count += changed
	}
	return count, nil
}
func (s *SQLStore) Pending(ctx context.Context, limit int) ([]*Record, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("pending job limit must be between 1 and 1000")
	}
	return s.read(ctx, "WHERE Status = ? AND (Deactivated = ? OR Deactivated IS NULL) AND (ExpiryTime IS NULL OR ExpiryTime > ?) ORDER BY CreationTime, ID LIMIT ?", xasync.StatusPending, false, time.Now().UTC(), limit)
}
