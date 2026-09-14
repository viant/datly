package builder

import (
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlmacro "github.com/viant/datly/sql/macro"
	"github.com/viant/sqlx"
)

// PreparedRelationBinder owns the route-preparation binding of one prepared
// relation query. It keeps the relation/query-specific state together so route
// preparation delegates relation SQL assembly instead of open-coding token
// scanning and root-arg stitching in the runtime shell.
type PreparedRelationBinder struct {
	Component         *spec.Component
	Relation          *data.Relation
	RootNonWindowSQL  string
	RootArgs          []any
	ParameterResolver sqlx.ParameterResolver
}

func (b PreparedRelationBinder) Bind() (string, []any, error) {
	if b.Relation == nil || b.Relation.Of == nil || b.Relation.Of.View == nil || b.Relation.Of.View.Spec.Source == nil {
		return "", nil, nil
	}
	if len(b.RootArgs) == 0 {
		return b.bindPreparedSQL()
	}
	authoredSQL := NormalizePreparedRelationSQL(b.Relation.Of.View.Spec.Source.SQL)
	if authoredSQL == "" {
		return "", nil, nil
	}
	if err := validatePreparedRelationAliases(b.Component, b.Relation, authoredSQL); err != nil {
		return "", nil, err
	}
	boundSQL, args, err := b.bindSegments(authoredSQL)
	if err != nil {
		return "", nil, err
	}
	return boundSQL, args, nil
}

func (b PreparedRelationBinder) bindPreparedSQL() (string, []any, error) {
	sqlText, err := PreparedRelationSQL(b.Component, b.Relation, b.RootNonWindowSQL)
	if err != nil {
		return "", nil, err
	}
	if sqlText == "" {
		return "", nil, nil
	}
	return bindParameters(dsql.PrepareExecutableSQL(sqlText, nil), b.ParameterResolver)
}

func (b PreparedRelationBinder) bindSegments(sqlText string) (string, []any, error) {
	if strings.TrimSpace(sqlText) == "" {
		return "", nil, nil
	}
	var builder strings.Builder
	var args []any
	remaining := sqlText
	for {
		token, idx := b.nextToken(remaining)
		if idx == -1 {
			break
		}
		prefix := remaining[:idx]
		boundPrefix, prefixArgs, err := bindParameters(dsql.PrepareExecutableSQL(prefix, nil), b.ParameterResolver)
		if err != nil {
			return "", nil, err
		}
		builder.WriteString(boundPrefix)
		args = append(args, prefixArgs...)
		if strings.TrimSpace(b.RootNonWindowSQL) == "" {
			return "", nil, MissingPreparedRelationRootSQL(b.Component, b.Relation)
		}
		builder.WriteString(b.RootNonWindowSQL)
		args = append(args, b.RootArgs...)
		remaining = remaining[idx+len(token):]
	}
	boundSuffix, suffixArgs, err := bindParameters(dsql.PrepareExecutableSQL(remaining, nil), b.ParameterResolver)
	if err != nil {
		return "", nil, err
	}
	builder.WriteString(boundSuffix)
	args = append(args, suffixArgs...)
	return builder.String(), args, nil
}

func bindParameters(sqlText string, resolver sqlx.ParameterResolver, positional ...any) (string, []any, error) {
	binder := sqlx.NewParameterBinder(resolver, positional...)
	boundSQL, args, err := binder.Bind(sqlText)
	if err != nil {
		return "", nil, err
	}
	if err = binder.Complete(); err != nil {
		return "", nil, err
	}
	return boundSQL, args, nil
}

func (b PreparedRelationBinder) nextToken(sqlText string) (string, int) {
	allowed := map[string]bool{}
	for _, token := range PreparedRelationNonWindowTokens(b.Component) {
		allowed[token] = true
	}
	for _, access := range sqlmacro.NonWindowSQLAccesses(sqlText) {
		if allowed[access.Raw] {
			return access.Raw, access.Start
		}
	}
	return "", -1
}
