package reader

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/viant/datly/internal/predicatereplay"
	"github.com/viant/datly/internal/readerlookup"
	"github.com/viant/datly/internal/sqlanalysis"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	rsql "github.com/viant/datly/sql/builder"
	"github.com/viant/datly/tag"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	"github.com/viant/sqlparser/node"
	"github.com/viant/sqlparser/query"
	sqlxio "github.com/viant/sqlx/io"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	xstate "github.com/viant/xdatly/state"
)

type lookupIdentity struct {
	index      []int
	predicates []*spec.Predicate
}

type emptyLookupRoute struct {
	identities []lookupIdentity
	predicates map[*spec.Predicate]bool
}

type emptyLookupPlan struct {
	routes     map[spec.RouteRef]emptyLookupRoute
	projection *dsql.Projection
}

// compileEmptyLookup retains only immutable route/accessor metadata. In
// particular neither identities nor existence decisions survive an invocation.
func compileEmptyLookup(config Config) (*emptyLookupPlan, error) {
	enabled, err := forbiddenNotFoundCheck(config)
	if err != nil || (enabled != nil && !*enabled) {
		return nil, err
	}
	// An explicit true also asks registration to diagnose an unsupported
	// declaration. Automatic classification needs proof before it can run.
	unproven := func(format string, args ...any) (*emptyLookupPlan, error) {
		if enabled != nil {
			return nil, fmt.Errorf(format, args...)
		}
		return nil, nil
	}
	component := config.Component
	marked := false
	for _, p := range component.Parameters {
		if p != nil {
			for _, predicate := range p.Predicates {
				if predicate != nil && predicate.Auth {
					marked = true
				}
			}
		}
	}
	if !marked {
		return unproven("forbiddenNotFoundCheck requires a marked authorization predicate")
	}
	outputType := config.OutputType
	for outputType != nil && outputType.Kind() == reflect.Pointer {
		outputType = outputType.Elem()
	}
	if outputType != nil && outputType.Kind() == reflect.Struct && config.Plan.OutputViewField != "" {
		field, ok := outputType.FieldByName(config.Plan.OutputViewField)
		if ok {
			if value := field.Tag.Get("onEmpty"); value != "" {
				if value != "empty" {
					return nil, fmt.Errorf("root onEmpty must be empty")
				}
				return nil, nil
			}
		}
	}
	for _, p := range component.Parameters {
		if p != nil && p.Source.Kind == "output" && p.Source.Name == "view" && reflect.StructTag(p.Tag).Get("onEmpty") == "empty" {
			return nil, nil
		}
	}

	if config.Plan.Root == nil || config.Plan.Root.View == nil {
		return unproven("forbiddenNotFoundCheck requires a root view")
	}
	keyNames := map[string]bool{}
	for _, column := range config.Plan.Root.View.Columns {
		if column == nil {
			continue
		}
		metadata := sqlxio.ParseTag(reflect.StructTag(column.Tag))
		if metadata != nil && metadata.PrimaryKey {
			name := column.Output
			if name == "" {
				name = column.Column
			}
			keyNames[name] = true
		}
	}
	for _, column := range config.Plan.Root.View.Spec.Columns {
		if column != nil && column.PrimaryKey {
			// Name is the Go member; Source/Output are the native SQL
			// projection identity. Generated Id and SQL ID are one key.
			name := column.Output
			if name == "" {
				name = column.Source
			}
			if name == "" {
				name = column.Name
			}
			keyNames[name] = true
		}
	}
	if len(keyNames) == 0 {
		return unproven("forbiddenNotFoundCheck requires root primary-key metadata")
	}
	// A collection route has no path input authority; do not analyze or reject
	// its SQL merely because a child predicate carries authorization metadata.
	pathBound := false
	for _, endpoint := range component.Routes {
		if endpoint == nil || !strings.EqualFold(endpoint.Method, "GET") {
			continue
		}
		template, err := route.CompilePathTemplate(endpoint.Path)
		if err != nil {
			return nil, err
		}
		for _, name := range template.Parameters() {
			for _, parameter := range component.Parameters {
				if parameter != nil && parameter.Source.Kind == "path" && parameter.Source.Name == name && parameter.Activation.Matches(endpoint.Path) {
					pathBound = true
				}
			}
		}
	}
	if !pathBound || config.Plan.Root.View.Spec.Source == nil || config.Plan.Root.View.Spec.Source.SQL == "" {
		return unproven("forbiddenNotFoundCheck requires a path-bound GET identity and root SQL")
	}

	analysisSource := config.Plan.Root.View.Spec.Source.SQL
	if native, ok := config.Plan.Root.Template.(interface {
		AnalysisSQL() string
		UsesPredicates() bool
	}); ok {
		if !native.UsesPredicates() {
			return unproven("forbiddenNotFoundCheck requires root predicate evaluation")
		}
		analysisSource = native.AnalysisSQL()
	}
	_, parsed, err := sqlanalysis.Checked(analysisSource, nil)
	if err != nil {
		return unproven("compile authorization empty lookup source: %w", err)
	}
	if parsed == nil {
		return unproven("authorization empty lookup requires a checked SELECT")
	}
	// Source-authored windows are ordinary result windows, including windows
	// nested inside a transparent root wrapper. They never imply denial.
	if identitySourceWindowed(parsed) {
		return nil, nil
	}
	lineage := (sqlparser.Lineage{Query: parsed}).Compile()
	keys := map[sqlparser.ColumnOrigin]bool{}
	for name := range keyNames {
		origin, ok := lineage.Lookup(name)
		if !ok {
			return unproven("authorization empty lookup primary key %s has no unambiguous source lineage", name)
		}
		keys[origin] = true
	}
	bindings, err := tag.NewBindingIndex(config.InputType)
	if err != nil {
		return nil, err
	}
	result := &emptyLookupPlan{routes: map[spec.RouteRef]emptyLookupRoute{}, projection: dsql.ExistenceProjection()}
routes:
	for _, endpoint := range component.Routes {
		if endpoint == nil || !strings.EqualFold(endpoint.Method, "GET") {
			continue
		}
		path, err := route.CompilePathTemplate(endpoint.Path)
		if err != nil {
			return nil, err
		}
		pathNames := map[string]bool{}
		for _, name := range path.Parameters() {
			pathNames[name] = true
		}
		matched := map[sqlparser.ColumnOrigin]bool{}
		lookup := emptyLookupRoute{predicates: make(map[*spec.Predicate]bool)}
		for _, p := range component.Parameters {
			if p == nil || p.Source.Kind != "path" || !pathNames[p.Source.Name] || !p.Activation.Matches(endpoint.Path) {
				continue
			}
			byOrigin := map[sqlparser.ColumnOrigin][]*spec.Predicate{}
			for _, predicate := range p.Predicates {
				if predicate == nil || predicate.Auth || (strings.ToLower(strings.TrimSpace(predicate.Name)) != "equal" && strings.ToLower(strings.TrimSpace(predicate.Name)) != "in") || len(predicate.Args) != 2 {
					continue
				}
				origin, ok := predicateColumnOrigin(parsed, predicate.Args[0], predicate.Args[1])
				if !ok || !keys[origin] {
					continue
				}
				matched[origin] = true
				byOrigin[origin] = append(byOrigin[origin], predicate)
				lookup.predicates[predicate] = true
			}
			if len(byOrigin) == 0 && enabled == nil {
				continue
			}
			field, ok, err := bindings.Resolve(p)
			if err != nil {
				return nil, err
			}
			if !ok {
				if enabled == nil {
					continue routes
				}
				return nil, fmt.Errorf("identity parameter %s has no input field", p.Name)
			}
			// Each distinct path input bound to a key needs evidence from the
			// primary query. Its repeated predicates can occur in separate groups.
			for _, definitions := range byOrigin {
				lookup.identities = append(lookup.identities, lookupIdentity{index: slices.Clone(field.Index), predicates: definitions})
			}
		}
		if len(matched) != len(keys) {
			continue
		}
		ref := spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}
		result.routes[ref] = lookup
	}
	if len(result.routes) == 0 {
		return unproven("forbiddenNotFoundCheck requires a GET route with a complete proven root identity")
	}
	if config.Plan.Root.Partitioner != nil {
		return unproven("authorization empty lookup does not support partitioned roots; use output tag onEmpty:empty")
	}

	return result, nil
}

// forbiddenNotFoundCheck is independent of predicate authorization metadata.
// Read the same root output metadata for authored and generated components.
func forbiddenNotFoundCheck(config Config) (*bool, error) {
	var enabled *bool
	read := func(metadata reflect.StructTag) error {
		value, present := metadata.Lookup("forbiddenNotFoundCheck")
		if !present {
			return nil
		}
		if value != "true" && value != "false" {
			return fmt.Errorf("forbiddenNotFoundCheck must be true or false")
		}
		flag := value == "true"
		if enabled != nil && *enabled != flag {
			return fmt.Errorf("conflicting root forbiddenNotFoundCheck metadata")
		}
		enabled = &flag
		return nil
	}
	output := config.OutputType
	for output != nil && output.Kind() == reflect.Pointer {
		output = output.Elem()
	}
	if output != nil && output.Kind() == reflect.Struct && config.Plan.OutputViewField != "" {
		if field, ok := output.FieldByName(config.Plan.OutputViewField); ok {
			if err := read(field.Tag); err != nil {
				return nil, err
			}
		}
	}
	for _, parameter := range config.Component.Parameters {
		if parameter != nil && parameter.Source.Kind == "output" && parameter.Source.Name == "view" {
			if err := read(reflect.StructTag(parameter.Tag)); err != nil {
				return nil, err
			}
		}
	}
	return enabled, nil
}

func (p *emptyLookupPlan) eligible(ctx context.Context, input reflect.Value, selector *xstate.Selector, root *ViewPlan) bool {
	if p == nil {
		return false
	}

	lookup, ok := p.routes[readerlookup.Route(ctx)]
	if !ok {
		return false
	}
	if selector != nil && (selector.Offset > 0 || selector.Page > 1) {
		return false
	}
	if controls := root.View.Spec.Source.Controls; controls != nil && (controls.Offset != nil && *controls.Offset > 0 || controls.Limit != nil && *controls.Limit == 0) {
		return false
	}
	for _, identity := range lookup.identities {
		value, err := input.Elem().FieldByIndexErr(identity.index)
		if err != nil {
			return false
		}
		for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
			if value.IsNil() {
				return false
			}
			value = value.Elem()
		}
		if (value.Kind() == reflect.Slice || value.Kind() == reflect.Array || value.Kind() == reflect.String) && value.Len() == 0 {
			return false
		}
	}

	return true
}

func (s *Service) classifyEmpty(ctx context.Context, session *Session, connection dsql.Connection, options []rsql.BuilderOption, selector *xstate.Selector, replay *predicatereplay.State) error {
	lookup := session.emptyLookup.routes[readerlookup.Route(ctx)]
	hasCriteria := selector != nil && strings.TrimSpace(selector.Criteria) != ""
	if hasCriteria {
		withoutCriteria := *selector
		withoutCriteria.Criteria = ""
		withoutCriteria.Placeholders = nil
		options = append(options, rsql.WithBuilderSelector(&withoutCriteria))
	}
	if hasCriteria || replay.HasOrdinaryFilters(lookup.predicates) {
		// A filter miss on an authorized identity is a normal not-found result.
		// Check that identity before testing existence without authorization.
		authorized, err := s.probeIdentity(ctx, session, connection, options, replay.Probe(ctx, lookup.predicates, true))
		if err != nil {
			return err
		}
		if authorized {
			return readerlookup.EmptyResult(false)
		}
	}
	exists, err := s.probeIdentity(ctx, session, connection, options, replay.Probe(ctx, lookup.predicates, false))
	if err != nil {
		return err
	}
	return readerlookup.EmptyResult(exists)
}

func (s *Service) probeIdentity(ctx context.Context, session *Session, connection dsql.Connection, options []rsql.BuilderOption, buildContext context.Context) (bool, error) {
	query, err := rsql.NewBuilder().Build(buildContext, options...)
	if err != nil {
		return false, fmt.Errorf("reader identity existence probe build: %w", err)
	}
	limit := 1
	query = &cache.ParmetrizedQuery{SQL: dsql.PrepareExecutableSQL(session.emptyLookup.projection.Render(query.SQL), &spec.ViewControls{Limit: &limit}), Args: query.Args}
	exists := false
	type existence struct {
		Exists int `sqlx:"datly_exists"`
	}
	scan := rowRead{view: session.Artifact.Root.View, cacheOnly: session.CacheOnly, options: []sqlxread.Option{sqlxread.WithQueryScope(session.QueryScope), sqlxread.WithCacheOnly(session.CacheOnly)}, newRow: func() any { return &existence{} }}
	if err := scan.query(ctx, rowQuery{db: connection.DB, tx: connection.Tx, query: query, read: session.rootRead, dialect: connection.Dialect, visit: func(any) error { exists = true; return nil }}); err != nil {
		return false, fmt.Errorf("reader identity existence probe: %w", err)
	}
	return exists, nil
}

// Resolve a predicate column through parser-owned lexical source scopes, then
// compare physical provenance with the root key. Alias spelling alone grants
// no identity; ambiguous occurrences are not eligible.
func predicateColumnOrigin(root *query.Select, alias, column string) (sqlparser.ColumnOrigin, bool) {
	var result sqlparser.ColumnOrigin
	matches := 0
	sqlparser.Traverse(root, func(n node.Node) bool {
		selectNode, ok := n.(*query.Select)
		if !ok || selectNode == nil {
			return true
		}
		candidate := *selectNode
		candidate.List = query.List{&query.Item{Expr: expr.NewSelector(alias + "." + column), Alias: "datly_identity"}}
		if origin, ok := (sqlparser.Lineage{Query: &candidate}).Compile().Lookup("datly_identity"); ok {
			result = origin
			matches++
		}
		return true
	})
	return result, matches == 1
}
func (p *emptyLookupPlan) usedIdentity(ctx context.Context, replay *predicatereplay.State) bool {
	if p == nil {
		return false
	}
	identities := p.routes[readerlookup.Route(ctx)].identities
	for _, identity := range identities {
		if !slices.ContainsFunc(identity.predicates, replay.HasPredicate) {
			return false
		}
	}
	return len(identities) > 0
}

// Only the row-producing source chain determines whether this is an empty
// page. Windows in scalar/EXISTS subqueries are part of source semantics.
func identitySourceWindowed(statement *query.Select) bool {
	if statement == nil {
		return false
	}
	if statement.ForUpdate {
		return true
	}
	if statement.Offset != nil {
		value, err := strconv.Atoi(statement.Offset.Value)
		if err != nil || value > 0 {
			return true
		}
	}
	if statement.Limit != nil {
		value, err := strconv.Atoi(statement.Limit.Value)
		if err != nil || value == 0 {
			return true
		}
	}
	if raw, ok := statement.From.X.(*expr.Raw); ok {
		if nested, ok := raw.X.(*query.Select); ok && identitySourceWindowed(nested) {
			return true
		}
	}
	if statement.Union != nil && identitySourceWindowed(statement.Union.X) {
		return true
	}
	return false
}
