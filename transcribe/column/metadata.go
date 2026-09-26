package column

import (
	"context"
	"database/sql"
	"sync"

	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/sink"
)

// Compilation owns metadata for one compilation's root and independent views.
// Catalog/schema configuration must remain stable for each *sql.DB pool during
// compilation. View graphs are mutated by discovery and must not be shared by
// concurrent compilations. A Refiner may be shared if its resolver is safe for
// concurrent use.
type Compilation struct {
	refiner  *Refiner
	metadata discoveryMetadata
}

// BeginCompilation starts isolated metadata reuse without retaining state on r.
// Discard the returned object when the compilation finishes, including failures.
func (r *Refiner) BeginCompilation() *Compilation {
	return &Compilation{refiner: r}
}

type discoveryMetadata struct {
	// Serialize cache fills; values are copied on return so SQLX cannot mutate
	// cached metadata. No table constraints or output columns are cached here.
	mu       sync.Mutex
	products map[*sql.DB]database.Product
	sessions map[*sql.DB]sink.Session
}

func (m *discoveryMetadata) product(ctx context.Context, db *sql.DB) (*database.Product, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if product, ok := m.products[db]; ok {
		return &product, nil
	}
	product, err := metadata.New().DetectProduct(ctx, db)
	if err != nil {
		return nil, err
	}
	if m.products == nil {
		m.products = make(map[*sql.DB]database.Product)
	}
	m.products[db] = *product
	copy := *product
	return &copy, nil
}

func (m *discoveryMetadata) session(ctx context.Context, db *sql.DB, product *database.Product) (*sink.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if session, ok := m.sessions[db]; ok {
		return &session, nil
	}
	session, err := config.Session(ctx, db, product)
	if err != nil {
		return nil, err
	}
	if m.sessions == nil {
		m.sessions = make(map[*sql.DB]sink.Session)
	}
	m.sessions[db] = *session
	copy := *session
	return &copy, nil
}
