package column

import (
	"context"
	"database/sql"
	"fmt"
	"go/token"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
	"github.com/viant/sqlx/option"
)

// PhysicalSourceIdentity is copied compilation evidence, never a runtime DB or
// mutable session handle. Catalog may be empty when native metadata reports it.
type PhysicalSourceIdentity struct {
	Connector string
	Catalog   string
	Schema    string
	Table     string
}

// PhysicalSourceIdentity discovers one unambiguous, unqualified physical table
// through the existing connector. Like compilation, it requires stable pool
// catalog/schema configuration; the reads are not a transactional snapshot.
func (r *Refiner) PhysicalSourceIdentity(ctx context.Context, connector, unqualifiedTable string) (PhysicalSourceIdentity, error) {
	if ctx == nil {
		return PhysicalSourceIdentity{}, fmt.Errorf("physical source context is required")
	}
	if err := ctx.Err(); err != nil {
		return PhysicalSourceIdentity{}, err
	}
	if r == nil || r.resolver == nil {
		return PhysicalSourceIdentity{}, fmt.Errorf("physical source DB resolver is required")
	}
	if connector == "" || strings.TrimSpace(connector) != connector || strings.ContainsAny(connector, "$\n\r") || !token.IsIdentifier(unqualifiedTable) || unqualifiedTable == "_" {
		return PhysicalSourceIdentity{}, fmt.Errorf("physical source requires exact connector and unqualified table")
	}
	db, err := r.resolver.ResolveDB(ctx, connector)
	if err != nil {
		return PhysicalSourceIdentity{}, err
	}
	if db == nil {
		return PhysicalSourceIdentity{}, fmt.Errorf("physical source DB is missing")
	}
	product, err := metadata.New().DetectProduct(ctx, db)
	if err != nil {
		return PhysicalSourceIdentity{}, err
	}
	var sessions []sink.Session
	if err = metadata.New().Info(ctx, db, info.KindSession, &sessions, product); err != nil {
		return PhysicalSourceIdentity{}, err
	}
	if len(sessions) != 1 || strings.TrimSpace(sessions[0].Schema) == "" {
		return PhysicalSourceIdentity{}, fmt.Errorf("physical source requires one nonempty native schema identity")
	}
	session := sessions[0]
	columns, err := config.Columns(ctx, &session, db, unqualifiedTable, product)
	if err != nil {
		return PhysicalSourceIdentity{}, err
	}
	if len(columns) == 0 {
		return PhysicalSourceIdentity{}, fmt.Errorf("physical source table metadata is absent: %s", unqualifiedTable)
	}
	for _, column := range columns {
		if (column.Table != "" && column.Table != unqualifiedTable) || (column.Schema != "" && column.Schema != session.Schema) || (column.Catalog != "" && column.Catalog != session.Catalog) {
			return PhysicalSourceIdentity{}, fmt.Errorf("physical source table metadata identity conflicts with native session")
		}
	}
	if err := ctx.Err(); err != nil {
		return PhysicalSourceIdentity{}, err
	}
	return PhysicalSourceIdentity{Connector: connector, Catalog: session.Catalog, Schema: session.Schema, Table: unqualifiedTable}, nil
}

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

// PhysicalSourceConstraints is detached, independently rediscovered compilation
// evidence. It carries no DB, sink, resolver, reflection or runtime handle.
type PhysicalSourceConstraints struct {
	Identity                        PhysicalSourceIdentity
	Product, ProductVersion, Engine string
	NativeColumns                   []PhysicalColumnFact
	PrimaryKeys, ForeignKeys        []PhysicalKeyFact
	Indexes                         []PhysicalIndexFact
	Projection                      []PhysicalProjectionFact
}
type PhysicalColumnFact struct {
	Name                        string
	Position                    int
	DatabaseType, Nullable, Key string
	Length, Precision, Scale    *int64
	Default                     *string
	AutoIncrement               *bool
	Primary                     bool
}
type PhysicalKeyFact struct {
	Name, Type, Catalog, Schema, Table                       string
	Position                                                 int
	Column, ReferenceTable, ReferenceColumn, ReferenceSchema string
	ConstrainPosition                                        int
	OnUpdate, OnDelete, OnMatch                              string
}
type PhysicalIndexFact struct {
	Catalog, Schema, TableSchema, Table, Name, Type, Origin string
	RawUnique                                               string
	Unique                                                  bool
	Partial                                                 string
	ColumnCount                                             int64
	Members                                                 []PhysicalIndexMemberFact
}
type PhysicalIndexMemberFact struct {
	Column                string
	Position              int
	Collation, Descending string
	PrefixLength          *int64
}
type PhysicalProjectionFact struct{ Physical, Projected, Result, Mapping string }

func (r *Refiner) PhysicalSourceConstraints(ctx context.Context, connector string, view *spec.View) (PhysicalSourceConstraints, error) {
	fail := func(err error) (PhysicalSourceConstraints, error) { return PhysicalSourceConstraints{}, err }
	if ctx == nil {
		return fail(fmt.Errorf("physical constraint context is required"))
	}
	if err := ctx.Err(); err != nil {
		return fail(err)
	}
	if r == nil || r.resolver == nil || view == nil || view.Source == nil {
		return fail(fmt.Errorf("physical constraints require a SQL view and resolver"))
	}
	if (strings.TrimSpace(view.Source.SQL) == "" && strings.TrimSpace(view.Source.URI) != "") || len(view.Source.Embeds) != 0 {
		return fail(fmt.Errorf("physical constraints require resolved SQL resource references"))
	}
	table := view.Source.Table
	if connector == "" || strings.TrimSpace(connector) != connector || strings.ContainsAny(connector, "$\n\r") || !token.IsIdentifier(table) || table == "_" || len(view.Relations) != 0 || view.SelfReference != nil {
		return fail(fmt.Errorf("physical constraints require exact connector and unqualified SQL leaf"))
	}
	db, err := r.resolver.ResolveDB(ctx, connector)
	if err != nil {
		return fail(err)
	}
	if db == nil {
		return fail(fmt.Errorf("physical constraint DB is missing"))
	}
	product, err := metadata.New().DetectProduct(ctx, db)
	if err != nil {
		return fail(err)
	}
	if product == nil || !((product.Name == "MySQL" && ((product.Major == 5 && product.Minor == 7) || product.Major == 8)) || (product.Name == "SQLite" && product.Major == 3)) {
		return fail(fmt.Errorf("physical constraints unsupported product/version"))
	}
	var sessions []sink.Session
	if err = metadata.New().Info(ctx, db, info.KindSession, &sessions, product); err != nil {
		return fail(err)
	}
	if len(sessions) != 1 || strings.TrimSpace(sessions[0].Schema) == "" {
		return fail(fmt.Errorf("physical constraints require one nonempty native schema"))
	}
	session := sessions[0]
	columns, err := config.Columns(ctx, &session, db, table, product)
	if err != nil {
		return fail(err)
	}
	if len(columns) == 0 {
		return fail(fmt.Errorf("physical constraint table columns are absent"))
	}
	result := PhysicalSourceConstraints{Identity: PhysicalSourceIdentity{Connector: connector, Catalog: session.Catalog, Schema: session.Schema, Table: table}, Product: product.Name, ProductVersion: fmt.Sprintf("%d.%d.%d", product.Major, product.Minor, product.Release)}
	exact := map[string]sink.Column{}
	folded := map[string]string{}
	for _, c := range columns {
		if c.Name == "" || (c.Table != "" && c.Table != table) || (c.Schema != "" && c.Schema != session.Schema) || (c.Catalog != "" && c.Catalog != session.Catalog) {
			return fail(fmt.Errorf("physical column identity conflicts with native table"))
		}
		key := normalizedName(c.Name)
		if _, ok := folded[key]; ok {
			return fail(fmt.Errorf("physical column identity is ambiguous"))
		}
		folded[key] = c.Name
		exact[c.Name] = c
		result.NativeColumns = append(result.NativeColumns, PhysicalColumnFact{Name: c.Name, Position: c.Position, DatabaseType: c.Type, Nullable: c.Nullable, Key: c.Key, Length: copyPhysicalValue(c.Length), Precision: copyPhysicalValue(c.Precision), Scale: copyPhysicalValue(c.Scale), Default: copyPhysicalValue(c.Default), AutoIncrement: copyPhysicalValue(c.IsAutoincrement), Primary: strings.EqualFold(c.Key, "PRI")})
	}
	if product.Name == "MySQL" {
		var tables []sink.Table
		if err = metadata.New().Info(ctx, db, info.KindTables, &tables, product, option.NewArgs(session.Catalog, session.Schema)); err != nil {
			return fail(err)
		}
		matches := 0
		for _, t := range tables {
			if t.Name != table {
				continue
			}
			matches++
			if t.Schema != session.Schema || (t.Catalog != "" && t.Catalog != session.Catalog) || t.Engine == nil || *t.Engine != "InnoDB" {
				return fail(fmt.Errorf("physical constraint engine/table authority is unsupported"))
			}
			result.Engine = *t.Engine
		}
		if matches != 1 {
			return fail(fmt.Errorf("physical constraint engine/table identity is absent or ambiguous"))
		}
	}
	var pk, fk []sink.Key
	args := option.NewArgs(session.Catalog, session.Schema, table)
	if err = metadata.New().Info(ctx, db, info.KindPrimaryKeys, &pk, product, args); err != nil {
		return fail(err)
	}
	if err = metadata.New().Info(ctx, db, info.KindForeignKeys, &fk, product, args); err != nil {
		return fail(err)
	}
	nativePK := []string{}
	for _, c := range result.NativeColumns {
		if c.Primary {
			nativePK = append(nativePK, c.Name)
		}
	}
	if len(pk) != 1 || len(nativePK) != 1 || pk[0].Column != nativePK[0] {
		return fail(fmt.Errorf("borrow_sql_row requires one complete physical primary key; composite/hidden membership rejected"))
	}
	expectedPosition := 0
	if product.Name == "MySQL" {
		expectedPosition = 1
	}
	if pk[0].Position != expectedPosition || pk[0].Type != "PRIMARY KEY" {
		return fail(fmt.Errorf("physical primary key position/type is unproved"))
	}
	allKeys := append(append([]sink.Key(nil), pk...), fk...)
	for _, k := range allKeys {
		if k.Name == "" || k.Table != table || (k.Schema != "" && k.Schema != session.Schema) || (k.Catalog != "" && k.Catalog != session.Catalog) {
			return fail(fmt.Errorf("physical key source identity conflicts"))
		}
		if _, ok := exact[k.Column]; !ok {
			return fail(fmt.Errorf("physical key column is unknown"))
		}
	}
	for _, k := range pk {
		result.PrimaryKeys = append(result.PrimaryKeys, copyPhysicalKey(k))
	}
	groups := map[string]bool{}
	for _, k := range fk {
		group := k.Catalog + "\x00" + k.Schema + "\x00" + k.Table + "\x00" + k.Name
		if product.Name == "SQLite" {
			group += fmt.Sprintf("\x00%d", k.ConstrainPosition)
		}
		if groups[group] || k.Position != expectedPosition || k.Type != "FOREIGN KEY" || k.ReferenceTable == "" || k.ReferenceColumn == "" || k.OnUpdate == "" || k.OnDelete == "" || k.OnMatch == "" {
			return fail(fmt.Errorf("physical foreign key is missing, composite, duplicate or ambiguous"))
		}
		knownRule := func(rule string) bool {
			switch rule {
			case "RESTRICT", "CASCADE", "SET NULL", "NO ACTION":
				return true
			case "SET DEFAULT":
				return product.Name == "SQLite"
			}
			return false
		}
		if !knownRule(k.OnUpdate) || !knownRule(k.OnDelete) || k.OnMatch != "NONE" {
			return fail(fmt.Errorf("physical foreign key action/match authority is unsupported"))
		}
		groups[group] = true
		if product.Name == "MySQL" && (k.ConstrainPosition <= 0 || k.ReferenceSchema == "" || k.ReferenceSchema != session.Schema) {
			return fail(fmt.Errorf("physical foreign key target/position is unproved or cross-schema"))
		}
		if product.Name == "SQLite" && (k.ConstrainPosition < 0 || (k.ReferenceSchema != "" && k.ReferenceSchema != session.Schema)) {
			return fail(fmt.Errorf("physical SQLite foreign key identity is unsupported"))
		}
		result.ForeignKeys = append(result.ForeignKeys, copyPhysicalKey(k))
	}
	var indexes []sink.Index
	if err = metadata.New().Info(ctx, db, info.KindIndexes, &indexes, product, args); err != nil {
		return fail(err)
	}
	indexNames := map[string]bool{}
	for _, idx := range indexes {
		if idx.Name == "" || indexNames[idx.Name] || idx.Table != table || (idx.TableSchema != "" && idx.TableSchema != session.Schema) || (idx.Schema != "" && idx.Schema != session.Schema) || (idx.Catalog != "" && idx.Catalog != session.Catalog) || idx.ColumnCount == nil || *idx.ColumnCount <= 0 {
			return fail(fmt.Errorf("physical index identity/count is absent or ambiguous"))
		}
		indexNames[idx.Name] = true
		if idx.Unique != "0" && idx.Unique != "1" {
			return fail(fmt.Errorf("physical index uniqueness is unavailable"))
		}
		unique := idx.Unique == "1"
		if product.Name == "MySQL" {
			unique = idx.Unique == "0"
		}
		if product.Name == "SQLite" && idx.Partial != "0" {
			return fail(fmt.Errorf("partial/unknown SQLite index rejected"))
		}
		fact := PhysicalIndexFact{Catalog: idx.Catalog, Schema: idx.Schema, TableSchema: idx.TableSchema, Table: idx.Table, Name: idx.Name, Type: idx.Type, Origin: idx.Origin, RawUnique: idx.Unique, Unique: unique, Partial: idx.Partial, ColumnCount: *idx.ColumnCount}
		var members []sink.Column
		if err = metadata.New().Info(ctx, db, info.KindIndex, &members, product, option.NewArgs(session.Catalog, session.Schema, table, idx.Name)); err != nil {
			return fail(err)
		}
		seen := map[int]bool{}
		memberNames := map[string]bool{}
		for _, member := range members {
			if product.Name == "SQLite" {
				if member.Key == "0" {
					continue
				}
				if member.Key != "1" {
					return fail(fmt.Errorf("SQLite index member kind unavailable"))
				}
			}
			if member.Table != table || member.Index != idx.Name || member.Name == "" || seen[member.IndexPosition] || memberNames[member.Name] {
				return fail(fmt.Errorf("physical index member is absent/duplicate"))
			}
			if _, ok := exact[member.Name]; !ok {
				return fail(fmt.Errorf("physical index member column is unknown"))
			}
			seen[member.IndexPosition] = true
			memberNames[member.Name] = true
			if (member.Schema != "" && member.Schema != session.Schema) || (member.Catalog != "" && member.Catalog != session.Catalog) || member.Collation == nil || *member.Collation == "" {
				return fail(fmt.Errorf("physical index member identity/collation unavailable"))
			}
			if product.Name == "MySQL" {
				if member.IndexPrefixLength == nil || *member.IndexPrefixLength != 0 || (*member.Collation != "A" && *member.Collation != "D") {
					return fail(fmt.Errorf("physical index prefix/direction unsupported"))
				}
			} else if member.Descending != "0" && member.Descending != "1" {
				return fail(fmt.Errorf("SQLite index direction unavailable"))
			}
			fact.Members = append(fact.Members, PhysicalIndexMemberFact{Column: member.Name, Position: member.IndexPosition, Collation: *member.Collation, Descending: member.Descending, PrefixLength: copyPhysicalValue(member.IndexPrefixLength)})
		}
		if int64(len(fact.Members)) != fact.ColumnCount {
			return fail(fmt.Errorf("physical index count/member mismatch; expression/hidden member rejected"))
		}
		sort.Slice(fact.Members, func(i, j int) bool { return fact.Members[i].Position < fact.Members[j].Position })
		for i, member := range fact.Members {
			if member.Position != i+expectedPosition {
				return fail(fmt.Errorf("physical index member positions are gapped"))
			}
		}
		if (product.Name == "MySQL" && idx.Name == "PRIMARY") || (product.Name == "SQLite" && idx.Origin == "pk") {
			if !fact.Unique || len(fact.Members) != 1 || fact.Members[0].Column != pk[0].Column {
				return fail(fmt.Errorf("physical primary index and key disagree"))
			}
		}
		result.Indexes = append(result.Indexes, fact)
	}
	result.Projection, err = physicalProjection(view, exact, folded)
	if err != nil {
		return fail(err)
	}
	projectedPK := 0
	for _, c := range view.Columns {
		if c.PrimaryKey {
			projectedPK++
			if c.Source != pk[0].Column {
				return fail(fmt.Errorf("projected primary key conflicts with physical key"))
			}
		}
	}
	if projectedPK != 1 {
		return fail(fmt.Errorf("one projected physical primary key is required"))
	}
	sort.Slice(result.NativeColumns, func(i, j int) bool { return result.NativeColumns[i].Name < result.NativeColumns[j].Name })
	sort.Slice(result.ForeignKeys, func(i, j int) bool {
		a, b := result.ForeignKeys[i], result.ForeignKeys[j]
		return fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s", a.Name, a.Schema, a.ConstrainPosition, a.Position, a.Column) < fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%s", b.Name, b.Schema, b.ConstrainPosition, b.Position, b.Column)
	})
	sort.Slice(result.Indexes, func(i, j int) bool { return result.Indexes[i].Name < result.Indexes[j].Name })
	if err = ctx.Err(); err != nil {
		return fail(err)
	}
	return result, nil
}
func copyPhysicalValue[T any](v *T) *T {
	if v == nil {
		return nil
	}
	copy := *v
	return &copy
}
func copyPhysicalKey(k sink.Key) PhysicalKeyFact {
	return PhysicalKeyFact{Name: k.Name, Type: k.Type, Catalog: k.Catalog, Schema: k.Schema, Table: k.Table, Position: k.Position, Column: k.Column, ReferenceTable: k.ReferenceTable, ReferenceColumn: k.ReferenceColumn, ReferenceSchema: k.ReferenceSchema, ConstrainPosition: k.ConstrainPosition, OnUpdate: k.OnUpdate, OnDelete: k.OnDelete, OnMatch: k.OnMatch}
}

func physicalProjection(view *spec.View, exact map[string]sink.Column, folded map[string]string) ([]PhysicalProjectionFact, error) {
	source := view.Source
	if strings.TrimSpace(source.SQL) != "" {
		parsed, err := parseProjectionAnalysis(source.SQL)
		if err != nil {
			return nil, err
		}
		table, _, err := sqlparser.SourceTable(parsed.From.X)
		if err != nil || table != source.Table || len(parsed.Joins) != 0 || parsed.Union != nil || len(parsed.WithSelects) != 0 || parsed.IsNested() {
			return nil, fmt.Errorf("physical projection requires one direct unqualified source")
		}
		outputs := map[string]bool{}
		wildcards := 0
		for _, item := range parsed.List {
			if item == nil {
				return nil, fmt.Errorf("physical projection item is absent")
			}
			if _, wild := projectionWildcard(item.Expr); wild {
				wildcards++
				if wildcards > 1 {
					return nil, fmt.Errorf("physical projection duplicate wildcard")
				}
				for name := range folded {
					if outputs[name] {
						return nil, fmt.Errorf("physical projection wildcard overlaps output")
					}
					outputs[name] = true
				}
				continue
			}
			c := sqlparser.NewColumn(item)
			name := normalizedName(c.Identity())
			if name == "" || outputs[name] || c.Expression != "" {
				return nil, fmt.Errorf("physical projection duplicate/computed output")
			}
			outputs[name] = true
		}
	}
	lineage, err := directProjectionLineage(source)
	if err != nil {
		return nil, err
	}
	var result []PhysicalProjectionFact
	outputs := map[string]bool{}
	physicalSeen := map[string]bool{}
	for _, c := range view.Columns {
		if c == nil || c.NameInferred || c.Expression != "" {
			return nil, fmt.Errorf("physical projection is not fresh direct SQL")
		}
		output := normalizedName(c.Name)
		physical, ok := lineage.direct[output]
		resultName := lineage.names[output]
		if !ok && c.Source != "" {
			output = normalizedName(c.Source)
			physical, ok = lineage.direct[output]
			resultName = lineage.names[output]
		}
		if !ok && c.Source != "" {
			// An outer Go rename can hide the original SQL output spelling.
			// Recover only a unique direct result for the already-proved source;
			// native emitted-field admission subsequently proves its actual alias.
			matches := 0
			for candidate, physicalName := range lineage.direct {
				if physicalName == normalizedName(c.Source) && !lineage.blocked[candidate] {
					matches++
					physical, resultName, output = physicalName, lineage.names[candidate], candidate
				}
			}
			ok = matches == 1
		}
		if !ok && lineage.wildcard && !lineage.blocked[output] {
			physical = output
			ok = physical != ""
			resultName = folded[physical]
		}
		name, known := folded[physical]
		if !ok || !known || lineage.blocked[output] || outputs[c.Name] || physicalSeen[name] || c.Source != name {
			return nil, fmt.Errorf("physical-to-projected source mapping is ambiguous/conflicting")
		}
		outputs[c.Name] = true
		physicalSeen[name] = true
		tag := sqlio.ParseTag(reflect.StructTag(c.Tag))
		mapping := tag.Column
		if mapping == "" {
			mapping = c.Source
		}
		parts := strings.Split(mapping, "|")
		if len(parts) > 2 || parts[0] != name || (len(parts) == 2 && parts[1] != resultName) {
			return nil, fmt.Errorf("authored SQLX mapping conflicts with physical/result identity")
		}
		if resultName == "" {
			resultName = name
		}
		result = append(result, PhysicalProjectionFact{Physical: name, Projected: c.Name, Result: resultName, Mapping: mapping})
	}
	return result, nil
}
