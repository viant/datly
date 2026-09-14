package builder

import (
	"fmt"
	"strings"

	"github.com/viant/datly/sql/criteria"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
)

func WithBuilderCriteriaCompiler(compiler *criteria.Compiler) BuilderOption {
	return func(options *builderOptions) { options.criteriaCompiler = compiler }
}

// prepareCriteria compiles invocation state without changing the caller's selector.
// Registered client views supply an explicit policy. Direct trusted builder
// callers may omit it, but still must identify columns via prepared metadata or
// an explicit SQL projection; an unqualified wildcard grants no column access.
func (b *Builder) prepareCriteria(options *builderOptions) error {
	if options.selector == nil || strings.TrimSpace(options.selector.Criteria) == "" {
		return nil
	}
	policy := options.selectorPolicy
	if policy == nil && options.view != nil {
		policy = options.view.Spec.Selector
	}
	if policy == nil && options.component != nil && options.component.RootView != nil {
		policy = options.component.RootView.Selector
	}
	if policy != nil && !policy.AllowCriteria {
		return fmt.Errorf("selector criteria is not allowed")
	}
	columns := map[string]criteria.Column{}
	var methods map[string]criteria.Method
	if options.criteriaCompiler != nil {
		for name, column := range options.criteriaCompiler.Columns {
			columns[name] = column
		}
		methods = options.criteriaCompiler.Methods
	}
	if options.criteriaCompiler == nil && options.view != nil {
		for _, col := range options.view.Columns {
			if col == nil {
				continue
			}
			target := col.Expression
			if target == "" {
				target = col.Column
			}
			if target == "" {
				target = col.Name
			}
			columns[col.Name] = criteria.Column{Expression: target}
			if col.Column != "" {
				columns[col.Column] = criteria.Column{Expression: target}
			}
		}
	}
	// Parse only the authored projection: the later source can legitimately
	// contain unexpanded Datly criteria markers or parameter expressions.
	projection := ""
	selectAt := sqltext.FindTopLevelKeyword(options.sqlText, "select", 0)
	if selectAt >= 0 {
		fromAt := sqltext.FindTopLevelKeyword(options.sqlText, "from", selectAt+6)
		if fromAt > selectAt {
			projection = options.sqlText[selectAt+6 : fromAt]
		}
	}
	parsed, err := sqlparser.ParseQuery("SELECT " + projection + " FROM criteria_source")
	if err == nil && parsed != nil {
		for _, item := range parsed.List {
			if item == nil {
				continue
			}
			switch item.Expr.(type) {
			case *expr.Ident, *expr.Selector:
				name := sqlparser.Stringify(item.Expr)
				if _, ok := columns[name]; !ok {
					columns[name] = criteria.Column{Expression: name}
				}
				if item.Alias != "" {
					if _, ok := columns[item.Alias]; !ok {
						columns[item.Alias] = columns[name]
					}
				}
			}
		}
	}
	if policy != nil && !(len(policy.Filterable) == 1 && strings.TrimSpace(string(policy.Filterable[0])) == "*") {
		allowed := map[string]criteria.Column{}
		for _, field := range policy.Filterable {
			for name, target := range columns {
				if strings.EqualFold(name, string(field)) {
					allowed[name] = target
				}
			}
		}
		columns = allowed
	}
	compiler := criteria.Compiler{Columns: columns, Methods: methods}
	sql, args, err := compiler.Compile(options.selector.Criteria, options.selector.Placeholders)
	if err != nil {
		return err
	}
	selector := *options.selector
	selector.Criteria, selector.Placeholders = sql, args
	options.selector = &selector
	return nil
}
