package column

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/viant/datly/constant"
	"io/fs"
	"reflect"
	"strings"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/config"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/metadata/sink"
)

// DBResolver resolves one configured connector for compile-time discovery.
type DBResolver interface {
	ResolveDB(context.Context, string) (*sql.DB, error)
}

// Connections is a static connector set suitable for command composition and
// tests. Production composition may supply its own DBResolver.
type Connections map[string]*sql.DB

func (c Connections) ResolveDB(_ context.Context, name string) (*sql.DB, error) {
	db := c[strings.TrimSpace(name)]
	if db == nil {
		return nil, fmt.Errorf("transcribe column: connector %q was not found", name)
	}
	return db, nil
}

// Refiner enriches canonical views with SQLX-discovered scalar columns.
type Refiner struct {
	resolver DBResolver
}

// TemplateInput is the compile-time contract used to evaluate parameterized
// SQL before SQLX column discovery. It contains no request or runtime session.
type TemplateInput struct {
	Const             *constant.Values
	Value             reflect.Value
	Variables         []sqltemplate.Variable
	ParameterResolver sqlx.ParameterResolver
	Predicate         exec.PredicateEvaluator
}

type expandedQuery struct {
	SQL  string
	Args []any
}

func New(resolver DBResolver) *Refiner {
	return &Refiner{resolver: resolver}
}

func (r *Refiner) Refine(ctx context.Context, component *spec.Component, resources fs.FS, input *TemplateInput) error {
	if err := r.RefineRoot(ctx, component, resources, input); err != nil {
		return err
	}
	return r.RefineViews(ctx, component, resources, input)
}

// RefineRoot discovers the root relation tree before generated input contracts
// are rebuilt for parameterized independent views.
func (r *Refiner) RefineRoot(ctx context.Context, component *spec.Component, resources fs.FS, input *TemplateInput) error {
	if r == nil || r.resolver == nil {
		return fmt.Errorf("transcribe column: DB resolver is required")
	}
	if component == nil {
		return fmt.Errorf("transcribe column: component is required")
	}
	connector := ""
	if component.Settings != nil {
		connector = strings.TrimSpace(component.Settings.DefaultConnector)
	}
	staged := TemplateInput{}
	if input != nil {
		staged = *input
	}
	var err error
	staged.Const, err = staged.Const.For(component)
	if err != nil {
		return err
	}
	if !staged.Value.IsValid() && !staged.Const.Empty() {
		staged.Value = reflect.ValueOf(struct{}{})
	}
	input = &staged
	visited := map[*spec.View]bool{}
	return r.refineView(ctx, component, component.RootView, connector, resources, input, nil, nil, visited)
}

// RefineViews discovers independent view trees against the rebuilt typed input.
func (r *Refiner) RefineViews(ctx context.Context, component *spec.Component, resources fs.FS, input *TemplateInput) error {
	if r == nil || r.resolver == nil {
		return fmt.Errorf("transcribe column: DB resolver is required")
	}
	if component == nil {
		return fmt.Errorf("transcribe column: component is required")
	}
	connector := ""
	if component.Settings != nil {
		connector = strings.TrimSpace(component.Settings.DefaultConnector)
	}
	staged := TemplateInput{}
	if input != nil {
		staged = *input
	}
	var err error
	staged.Const, err = staged.Const.For(component)
	if err != nil {
		return err
	}
	if !staged.Value.IsValid() && !staged.Const.Empty() {
		staged.Value = reflect.ValueOf(struct{}{})
	}
	input = &staged
	visited := map[*spec.View]bool{}
	for _, view := range component.Views {
		if err := r.refineView(ctx, component, view, connector, resources, input, nil, nil, visited); err != nil {
			return err
		}
	}
	return nil
}

func (r *Refiner) refineView(ctx context.Context, component *spec.Component, view *spec.View, inheritedConnector string, resources fs.FS, input *TemplateInput, parent *expandedQuery, parentAliases []string, visited map[*spec.View]bool) error {
	if view == nil || visited[view] {
		return nil
	}
	visited[view] = true
	connector := inheritedConnector
	if view.Source != nil && view.Source.Bindings != nil && strings.TrimSpace(view.Source.Bindings.Connector) != "" {
		connector = strings.TrimSpace(view.Source.Bindings.Connector)
	}
	var query *expandedQuery
	if view.Source != nil {
		var err error
		query, err = r.discover(ctx, view, connector, resources, input, parent, parentAliases)
		if err != nil {
			return fmt.Errorf("transcribe column: refine view %q: %w", view.Name, err)
		}
	}
	aliases := templateAliases(view)
	if view == component.RootView {
		aliases = appendAlias(aliases, component.Name)
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		if err := r.refineView(ctx, component, relation.View, connector, resources, input, query, aliases, visited); err != nil {
			return err
		}
	}
	return nil
}

func (r *Refiner) discover(ctx context.Context, view *spec.View, connector string, resources fs.FS, input *TemplateInput, parent *expandedQuery, parentAliases []string) (*expandedQuery, error) {
	if err := validateColumns("canonical", view.Columns); err != nil {
		return nil, err
	}
	source := view.Source.Clone()
	if input != nil {
		resources = input.Const.Resources(resources)
	}
	if err := dsql.ResolveSource(view.Name, source, resources); err != nil {
		return nil, err
	}
	authoredSource := source.Clone()
	if strings.TrimSpace(source.SQL) == "" && strings.TrimSpace(source.Table) == "" {
		return &expandedQuery{}, nil
	}
	if strings.TrimSpace(connector) == "" {
		return nil, fmt.Errorf("connector is required")
	}
	if input != nil {
		validationSQL, validationErr := schemaDiscoverySQL(source.SQL)
		if validationErr != nil {
			return nil, validationErr
		}
		names := append([]string(nil), parentAliases...)
		for _, variable := range input.Variables {
			names = append(names, variable.Name)
		}
		renderer := sqltemplate.ConstantRenderer{Values: input.Const, Variables: names}
		if err := renderer.Validate(validationSQL); err != nil {
			return nil, err
		}
		if err := renderer.Validate("SELECT * FROM " + source.Table); err != nil {
			return nil, err
		}
	}
	db, err := r.resolver.ResolveDB(ctx, connector)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("connector %q returned a nil DB", connector)
	}
	if input != nil {
		source.Table, err = (sqltemplate.ConstantRenderer{Values: input.Const}).Identifier(source.Table)
		if err != nil {
			return nil, err
		}
	}
	dialect, err := config.Dialect(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("resolve SQL dialect: %w", err)
	}
	evaluationSource := source.Clone()
	evaluationSource.SQL, err = schemaDiscoverySQL(evaluationSource.SQL)
	if err != nil {
		return nil, err
	}
	evaluated, err := evaluateSource(ctx, evaluationSource, dialect, input, parent, parentAliases)
	if err != nil {
		return nil, fmt.Errorf("evaluate schema-discovery source: %w", err)
	}
	source.SQL = evaluated.SQL
	source.SQL, err = schemaDiscoverySQL(source.SQL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(source.Table) == "" {
		if table := directSourceTable(evaluated.SQL); table != "" {
			source.Table = table
			// Resource-backed and inline SQL have identical table authority once
			// the evaluated query proves one direct physical source. Persist that
			// fact for writer planning; readers do not depend on it.
			view.Source.Table = table
		}
	}
	// Metadata reads follow the table actually selected by the evaluated query.
	// This also handles existing $Unsafe table constants without reversing or
	// overwriting their authored SQL or canonical table metadata.
	if source.Table != "" {
		if parsed, parseErr := sqlparser.ParseQuery(evaluated.SQL); parseErr == nil && parsed != nil && len(parsed.WithSelects) == 0 && parsed.Union == nil {
			if table, _, tableErr := sqlparser.SourceTable(parsed.From.X); tableErr == nil && table != "" {
				source.Table = table
			}
		}
	}
	var constraints map[string]tableConstraint
	if table := strings.TrimSpace(source.Table); table != "" && !strings.Contains(table, "$") {
		constraints, err = loadTableConstraints(ctx, db, table)
		if err != nil {
			return nil, err
		}
		beforeIdentity := authoredSource.SQL
		if !strings.Contains(authoredSource.SQL, "${predicate.") {
			if err := retainNamedIdentity(view, authoredSource, constraints); err != nil {
				return nil, fmt.Errorf("retain named identity: %w", err)
			}
		}
		if authoredSource.SQL != beforeIdentity {
			// Identity augmentation owns authored SQL. Re-evaluate its changed copy;
			// never feed expanded SQL back into the source-persistence owner.
			recheckSource := authoredSource.Clone()
			recheckSource.SQL, err = schemaDiscoverySQL(recheckSource.SQL)
			if err != nil {
				return nil, err
			}
			evaluated, err = evaluateSource(ctx, recheckSource, dialect, input, parent, parentAliases)
			if err != nil {
				return nil, fmt.Errorf("re-evaluate named identity source: %w", err)
			}
			source.SQL = evaluated.SQL
		}
	}
	query, err := discoveryQuery(source)
	if err != nil {
		return nil, fmt.Errorf("build schema-discovery query: %w", err)
	}
	if strings.TrimSpace(query) == "" {
		return evaluated, nil
	}
	detected, err := r.detectColumns(ctx, db, view, query, evaluated.Args...)
	if err != nil {
		return nil, fmt.Errorf("SQLX discovery failed for %q: %w", query, err)
	}
	columns, err := canonicalColumns(detected, view.Groupable != nil && *view.Groupable)
	if err != nil {
		return nil, err
	}
	projected := make([]string, 0, len(columns))
	for _, column := range columns {
		projected = append(projected, column.Name)
	}
	if err := ValidateProjectionAnnotations(view, projected); err != nil {
		return nil, err
	}
	// Successful database discovery can materialize a named outer wildcard
	// even when the local parser cannot enumerate its inner dialect SQL.
	// The authored source is retained exactly. A predicate template can make
	// SQL parser projection rewriting unsafe even though discovery has already
	// produced canonical columns for dynamic runtime shapes.
	if !strings.Contains(view.Source.SQL, "${predicate.") {
		resolvedSQL, changed, err := (dsql.SelectorProjection{SQL: view.Source.SQL}).ResolveDiscoveredColumns(projected, dialect)
		if err != nil {
			return nil, err
		}
		if changed {
			view.Source.SQL = resolvedSQL
		}
	}
	view.Columns = mergeColumns(view.Columns, columns)
	if table := strings.TrimSpace(source.Table); table != "" && !strings.Contains(table, "$") {
		lineage, err := directProjectionLineage(source)
		if err != nil {
			return nil, fmt.Errorf("resolve table projection lineage: %w", err)
		}
		applyTableConstraints(view.Columns, constraints, lineage)
	}
	return evaluated, nil
}

// schemaDiscoverySQL removes unresolved predicate expansions only from the
// zero-row metadata query. Runtime/authored SQL remains owned by the original
// view source and evaluated result.
func schemaDiscoverySQL(SQL string) (string, error) {
	const prefix = "${predicate."
	result := SQL
	for {
		start := strings.Index(result, prefix)
		if start < 0 {
			return result, nil
		}
		end := strings.IndexByte(result[start+len(prefix):], '}')
		if end < 0 {
			return "", fmt.Errorf("unterminated predicate expression in schema discovery SQL")
		}
		end += start + len(prefix)
		result = result[:start] + result[end+1:]
	}
}

func directSourceTable(SQL string) string {
	parsed, err := sqlparser.ParseQuery(strings.TrimSpace(SQL))
	if err != nil {
		return ""
	}
	// A set operation, including one nested inside a derived source, does not
	// establish one explicit physical table. Lineage can legitimately converge
	// on the same table across branches, but that is insufficient writer or
	// metadata-lookup authority.
	hasUnion := false
	sqlparser.Traverse(parsed, func(current node.Node) bool {
		if selection, ok := current.(*query.Select); ok && selection.Union != nil {
			hasUnion = true
			return false
		}
		return true
	})
	if hasUnion {
		return ""
	}
	lineage := sqlparser.Lineage{Query: parsed}
	if table := strings.TrimSpace(lineage.RootTable()); table != "" {
		return table
	}
	// A derived source has no direct root, but the parser can still prove that
	// every exposed direct column originates from one physical table.
	table := ""
	for _, origin := range lineage.Columns() {
		candidate := strings.TrimSpace(origin.Table)
		if candidate == "" {
			continue
		}
		if table != "" && !strings.EqualFold(table, candidate) {
			return ""
		}
		table = candidate
	}
	if table == "" && parsed.Union == nil {
		candidate := strings.TrimSpace(sqlparser.TableName(parsed))
		for _, with := range parsed.WithSelects {
			if with != nil && strings.EqualFold(strings.TrimSpace(with.Alias), candidate) {
				return ""
			}
		}
		table = candidate
	}
	return table
}

func evaluateSource(ctx context.Context, source *spec.ViewSource, dialect *info.Dialect, input *TemplateInput, parent *expandedQuery, parentAliases []string) (*expandedQuery, error) {
	if source == nil {
		return &expandedQuery{}, nil
	}
	sqlText := strings.TrimSpace(source.SQL)
	if sqlText == "" && strings.TrimSpace(source.Table) != "" {
		sqlText = "SELECT * FROM " + strings.TrimSpace(source.Table)
	}
	if sqlText == "" {
		return &expandedQuery{}, nil
	}
	var (
		value     reflect.Value
		variables []sqltemplate.Variable
		resolver  sqlx.ParameterResolver
		predicate exec.PredicateEvaluator
	)
	if input != nil {
		value = input.Value
		variables = input.Variables
		resolver = input.ParameterResolver
		predicate = input.Predicate
	}
	var inputType reflect.Type
	if value.IsValid() {
		inputType = value.Type()
	}
	for inputType != nil && inputType.Kind() == reflect.Pointer {
		inputType = inputType.Elem()
	}
	program, err := (sqltemplate.Compiler{
		Const: func() *constant.Values {
			if input != nil {
				return input.Const
			}
			return nil
		}(),
		Source: sqlText, InputType: inputType, Variables: variables,
		NonWindowAliases: parentAliases, Predicate: predicate,
	}).Compile()
	if err != nil {
		return nil, err
	}
	positional := []any(nil)
	if program != nil {
		if !value.IsValid() {
			return nil, fmt.Errorf("SQL template requires a compile-time input contract")
		}
		viewInput := sqltemplate.ViewInput{Dialect: dialect, ExcludeParent: true}
		if parent != nil {
			viewInput.NonWindowSQL = parent.SQL
			viewInput.NonWindowArgs = append([]any(nil), parent.Args...)
		}
		result, err := program.Evaluate(ctx, sqltemplate.Invocation{Input: value, View: viewInput})
		if err != nil {
			return nil, err
		}
		sqlText = result.SQL
		positional = result.Args
	}
	binder := sqlx.NewParameterBinder(resolver, positional...)
	boundSQL, args, err := binder.Bind(sqlText)
	if err != nil {
		return nil, fmt.Errorf("bind discovery SQL: %w", err)
	}
	if err = binder.Complete(); err != nil {
		return nil, fmt.Errorf("bind discovery SQL: %w", err)
	}
	return &expandedQuery{SQL: boundSQL, Args: args}, nil
}

func templateAliases(view *spec.View) []string {
	if view == nil {
		return nil
	}
	result := appendAlias(nil, view.Name)
	return appendAlias(result, view.Key.Name)
}

func appendAlias(aliases []string, candidate string) []string {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return aliases
	}
	for _, alias := range aliases {
		if alias == candidate {
			return aliases
		}
	}
	return append(aliases, candidate)
}

func canonicalColumns(source []*sink.Column, groupable bool) ([]*spec.Column, error) {
	result := make([]*spec.Column, 0, len(source))
	for _, item := range source {
		if item == nil {
			continue
		}
		name := strings.TrimSpace(item.Name)
		if name == "" {
			return nil, fmt.Errorf("discovered column name is required")
		}
		columnType := io.NormalizeColumnType(item.ScanType(), item.Type)
		typeRef, nullable := canonicalType(columnType, item.IsNullable())
		columnGroupable := groupable
		result = append(result, &spec.Column{
			Name: name, Source: name, DatabaseType: strings.TrimSpace(item.Type),
			Type: typeRef, Nullable: nullable, Groupable: &columnGroupable,
		})
	}
	if err := validateColumns("discovered", result); err != nil {
		return nil, err
	}
	return result, nil
}

func canonicalType(source reflect.Type, nullable bool) (spec.TypeRef, bool) {
	for source != nil && source.Kind() == reflect.Ptr {
		nullable = true
		source = source.Elem()
	}
	if source == nil || source.Kind() == reflect.Interface {
		return spec.TypeRef{Name: "any"}, nullable
	}
	if source.Name() != "" {
		return spec.TypeRef{Package: source.PkgPath(), Name: source.Name()}, nullable
	}
	return spec.TypeRef{Name: source.String()}, nullable
}

func mergeColumns(base, discovered []*spec.Column) []*spec.Column {
	if len(base) == 0 {
		return discovered
	}
	byName := map[string]*spec.Column{}
	for _, column := range discovered {
		if column != nil {
			byName[strings.ToLower(strings.TrimSpace(column.Name))] = column
		}
	}
	result := make([]*spec.Column, 0, len(base)+len(discovered))
	for _, column := range base {
		if column == nil {
			continue
		}
		cloned := column.Clone()
		key := strings.ToLower(strings.TrimSpace(column.Name))
		fresh := byName[key]
		if fresh == nil && column.Source != "" {
			key = strings.ToLower(strings.TrimSpace(column.Source))
			fresh = byName[key]
		}
		if fresh != nil {
			cloned.Source = firstValue(cloned.Source, fresh.Source)
			cloned.DatabaseType = firstValue(fresh.DatabaseType, cloned.DatabaseType)
			if cloned.Type.IsZero() {
				cloned.Type = fresh.Type
			}
			cloned.Nullable = fresh.Nullable
			cloned.NotNull = fresh.NotNull
			if cloned.Groupable == nil && fresh.Groupable != nil {
				value := *fresh.Groupable
				cloned.Groupable = &value
			}
			cloned.PrimaryKey = cloned.PrimaryKey || fresh.PrimaryKey
			cloned.AutoIncrement = cloned.AutoIncrement || fresh.AutoIncrement
			cloned.Unique = cloned.Unique || fresh.Unique
			if cloned.Default == nil && fresh.Default != nil {
				value := *fresh.Default
				cloned.Default = &value
			}
			delete(byName, key)
		}
		result = append(result, cloned)
	}
	for _, column := range discovered {
		if column == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(column.Name))
		if byName[key] == nil {
			continue
		}
		result = append(result, column)
		delete(byName, key)
	}
	return result
}

func validateColumns(origin string, columns []*spec.Column) error {
	seenNames := map[string]string{}
	seenFields := map[string]string{}
	for _, column := range columns {
		if column == nil {
			continue
		}
		name := strings.TrimSpace(column.Name)
		if name == "" {
			return fmt.Errorf("%s column name is required", origin)
		}
		key := strings.ToLower(name)
		if previous := seenNames[key]; previous != "" {
			return fmt.Errorf("%s columns %q and %q are ambiguous", origin, previous, name)
		}
		seenNames[key] = name
		fieldName := typecatalog.FieldName(name)
		if fieldName == "" {
			return fmt.Errorf("%s column %q has no canonical Go field name", origin, name)
		}
		if previous := seenFields[fieldName]; previous != "" {
			return fmt.Errorf("%s columns %q and %q map to Go field %q", origin, previous, name, fieldName)
		}
		seenFields[fieldName] = name
	}
	return nil
}

func firstValue(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
