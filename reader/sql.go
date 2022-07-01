package reader

import (
	"fmt"
	"github.com/viant/datly/reader/metadata"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	rdata "github.com/viant/toolbox/data"
	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/parser"
	"strconv"
	"strings"
)

const (
	selectFragment      = "SELECT "
	separatorFragment   = ", "
	fromFragment        = " FROM "
	asFragment          = " AS "
	limitFragment       = " LIMIT "
	orderByFragment     = " ORDER BY "
	offsetFragment      = " OFFSET "
	inFragment          = " IN ("
	andFragment         = " AND ("
	placeholderFragment = "?"
	encloseFragment     = ")"
)

type (

	//Builder represent SQL Builder
	Builder struct{}

	//BatchData groups view needed to use various view.MatchStrategy
	BatchData struct {
		ColumnName     string
		Parent         int
		ParentReadSize int

		Values      []interface{}
		ValuesBatch []interface{}
	}
)

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
func (b *Builder) Build(aView *view.View, selector *view.Selector, batchData *BatchData, relation *view.Relation, parentOfAclView *view.View) (string, []interface{}, error) {
	template, err := aView.Template.EvaluateSource(selector.Parameters.Values, selector.Parameters.Has, parentOfAclView)
	if err != nil {
		return "", nil, err
	}

	if aView.Template.IsActualTemplate() && relation != nil && aView.ShouldTryDiscover() {
		template = metadata.EnrichWithDiscover(template, true)
	}

	sb := strings.Builder{}
	sb.WriteString(selectFragment)
	if err = b.appendColumns(&sb, aView, selector, relation); err != nil {
		return "", nil, err
	}

	if err = b.appendRelationColumn(&sb, aView, selector, relation); err != nil {
		return "", nil, err
	}

	sb.WriteString(fromFragment)
	sb.WriteString(template)
	b.appendViewAlias(&sb, aView)

	columnsInMeta := hasKeyword(template, keywords.ColumnsIn)
	commonParams := view.CommonParams{}

	criteriaMeta := hasKeyword(template, keywords.Criteria)
	hasCriteria := criteriaMeta.has()

	b.updateColumnsIn(&commonParams, aView, relation, batchData, columnsInMeta, hasCriteria)

	if err = b.updatePagination(&commonParams, aView, selector); err != nil {
		return "", nil, err
	}

	if err = b.updateCriteria(&commonParams, columnsInMeta, selector); err != nil {
		return "", nil, err
	}

	if !hasCriteria {
		sb.WriteString(" ")
		if strings.TrimSpace(commonParams.WhereClause) != "" {
			sb.WriteString(keywords.WhereCriteria)
			sb.WriteString(" ")
			sb.WriteString(keywords.AndSelectorCriteria)
		} else {
			sb.WriteString(keywords.WhereSelectorCriteria)
			sb.WriteString(" ")
		}
	} else {
		sb.WriteString(keywords.WhereSelectorCriteria)
	}

	hasPagination := strings.Contains(template, keywords.Pagination)
	if !hasPagination {
		sb.WriteString(" ")
		sb.WriteString(keywords.Pagination)
		sb.WriteString(" ")
	}

	placeholders, err := b.getInitialPlaceholders(aView, selector)
	if err != nil {
		return "", nil, err
	}

	SQL, err := b.expand(sb.String(), aView, selector, commonParams, batchData, &placeholders)
	if err != nil {
		return "", nil, err
	}

	return SQL, placeholders, err
}

func (b *Builder) appendColumns(sb *strings.Builder, aView *view.View, selector *view.Selector, relation *view.Relation) error {
	if len(selector.Columns) == 0 {
		b.appendViewColumns(sb, aView)
		return nil
	}

	return b.appendSelectorColumns(sb, aView, selector, relation)
}

func (b *Builder) appendSelectorColumns(sb *strings.Builder, view *view.View, selector *view.Selector, relation *view.Relation) error {
	for i, column := range selector.Columns {
		viewColumn, ok := view.ColumnByName(column)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", column, view.Name)
		}

		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		sb.WriteString(" ")
		sb.WriteString(viewColumn.SqlExpression())
	}

	return nil
}

func (b *Builder) viewAlias(view *view.View) string {
	var alias string
	if view.Alias != "" {
		alias = view.Alias + "."
	}
	return alias
}

func (b *Builder) appendViewColumns(sb *strings.Builder, view *view.View) {
	alias := b.viewAlias(view)

	for i, column := range view.Columns {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}

		sb.WriteString(" ")
		if column.Name == column.SqlExpression() {
			sb.WriteString(alias)
		}

		sb.WriteString(column.SqlExpression())
	}
}

func (b *Builder) appendViewAlias(sb *strings.Builder, view *view.View) {
	if view.Alias == "" {
		return
	}

	sb.WriteString(asFragment)
	sb.WriteString(view.Alias)
}

func (b *Builder) updatePagination(params *view.CommonParams, view *view.View, selector *view.Selector) error {
	sb := strings.Builder{}
	if err := b.appendOrderBy(&sb, view, selector); err != nil {
		return err
	}
	b.appendLimit(&sb, view, selector)
	b.appendOffset(&sb, selector)
	params.Pagination = sb.String()
	return nil
}

func (b *Builder) appendLimit(sb *strings.Builder, view *view.View, selector *view.Selector) {
	if selector.Limit != 0 && (selector.Limit < view.Selector.Limit || view.Selector.Limit == 0) {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(selector.Limit))
		return
	}

	if view.Selector.Limit != 0 {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(view.Selector.Limit))
		return
	}
}

func (b *Builder) appendOffset(sb *strings.Builder, selector *view.Selector) {
	if selector.Offset == 0 {
		return
	}

	sb.WriteString(offsetFragment)
	sb.WriteString(strconv.Itoa(selector.Offset))
}

func (b *Builder) expand(sql string, aView *view.View, selector *view.Selector, params view.CommonParams, batchData *BatchData, placeholders *[]interface{}) (string, error) {
	block, err := parser.Parse([]byte(sql))
	if err != nil {
		return "", err
	}

	replacement := rdata.Map{}

	for _, statement := range block.Stmt {
		key, val, err := b.prepareExpanded(statement, params, aView, selector, batchData, placeholders)
		if err != nil {
			return "", err
		}

		if key == "" {
			continue
		}

		replacement.SetValue(key, val)
	}

	return replacement.ExpandAsText(sql), err
}

func (b *Builder) prepareExpanded(statement ast.Statement, params view.CommonParams, aView *view.View, selector *view.Selector, batchData *BatchData, placeholders *[]interface{}) (string, string, error) {
	switch actual := statement.(type) {
	case *expr.Select:
		key := extractSelectorName(actual.FullName)
		mapKey, mapValue, err := b.replacementEntry(key, params, aView, selector, batchData, placeholders)
		if err != nil {
			return "", "", err
		}

		return mapKey, mapValue, nil
	}

	return "", "", nil
}

func (b *Builder) replacementEntry(key string, params view.CommonParams, aView *view.View, selector *view.Selector, batchData *BatchData, placeholders *[]interface{}) (string, string, error) {
	switch key {
	case keywords.Pagination[1:]:
		return key, params.Pagination, nil
	case keywords.Criteria[1:]:
		criteriaExpanded, err := b.expand(params.WhereClause, aView, selector, params, batchData, placeholders)
		if err != nil {
			return "", "", err
		}

		return key, criteriaExpanded, nil
	case keywords.ColumnsIn[1:]:
		*placeholders = append(*placeholders, batchData.ValuesBatch...)
		return key, params.ColumnsIn, nil
	case keywords.SelectorCriteria[1:]:
		*placeholders = append(*placeholders, selector.Placeholders...)
		return key, selector.Criteria, nil
	default:
		if strings.HasPrefix(key, keywords.WherePrefix) {
			_, aValue, err := b.replacementEntry(key[len(keywords.WherePrefix):], params, aView, selector, batchData, placeholders)
			if err != nil {
				return "", "", err
			}

			return b.valueWithPrefix(key, aValue, " WHERE ", false)
		}

		if strings.HasPrefix(key, keywords.AndPrefix) {
			_, aValue, err := b.replacementEntry(key[len(keywords.AndPrefix):], params, aView, selector, batchData, placeholders)
			if err != nil {
				return "", "", err
			}

			return b.valueWithPrefix(key, aValue, " AND ", true)
		}

		if strings.HasPrefix(key, keywords.OrPrefix) {
			_, aValue, err := b.replacementEntry(key[len(keywords.OrPrefix):], params, aView, selector, batchData, placeholders)
			if err != nil {
				return "", "", err
			}

			return b.valueWithPrefix(key, aValue, " OR ", true)
		}

		accessor, err := aView.Template.AccessorByName(key)
		if err != nil {
			return "", "", err
		}

		value, err := accessor.Value(selector.Parameters.Values)
		if err != nil {
			return "", "", err
		}

		*placeholders = append(*placeholders, value)
		return key, "?", nil
	}
}

func (b *Builder) valueWithPrefix(key string, aValue, prefix string, wrapWithParentheses bool) (string, string, error) {
	if aValue == "" {
		return key, "", nil
	}

	if wrapWithParentheses {
		return key, prefix + "(" + aValue + ")", nil
	}

	return key, prefix + aValue, nil
}

func (b *Builder) updateCriteria(params *view.CommonParams, columnsInMeta *reservedMeta, selector *view.Selector) error {
	sb := strings.Builder{}
	hasColumnsIn := columnsInMeta.has()

	if !hasColumnsIn && params.ColumnsIn != "" {
		b.appendCriteria(&sb, keywords.ColumnsIn, false)
	}

	params.WhereClause = sb.String()
	return nil
}

func (b *Builder) appendCriteria(sb *strings.Builder, criteria string, addAnd bool) {
	if addAnd {
		sb.WriteString(andFragment)
	}

	sb.WriteString(criteria)

	if addAnd {
		sb.WriteString(encloseFragment)
	}
}

func (b *Builder) updateColumnsIn(params *view.CommonParams, view *view.View, relation *view.Relation, batchData *BatchData, columnsInMeta *reservedMeta, hasCriteria bool) {
	columnsIn := columnsInMeta.has()

	if batchData == nil || batchData.ColumnName == "" {
		return
	}

	alias := b.viewAlias(view)
	if hasCriteria || columnsIn {
		if relation.ColumnAlias != "" {
			alias = relation.ColumnAlias + "."
		} else {
			alias = ""
		}
	}

	sb := strings.Builder{}
	sb.WriteString(" ")
	sb.WriteString(alias)
	sb.WriteString(batchData.ColumnName)
	sb.WriteString(inFragment)

	for i := range batchData.ValuesBatch {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(placeholderFragment)
	}
	sb.WriteString(encloseFragment)
	params.ColumnsIn = sb.String()
}

func (b *Builder) appendOrderBy(sb *strings.Builder, view *view.View, selector *view.Selector) error {
	if selector.OrderBy != "" {
		col, ok := view.ColumnByName(selector.OrderBy)
		if !ok {
			return fmt.Errorf("not found column %v at view %v", selector.OrderBy, view.Name)
		}

		sb.WriteString(orderByFragment)
		sb.WriteString(col.Name)
		return nil
	}

	if view.Selector.OrderBy != "" {
		sb.WriteString(orderByFragment)
		sb.WriteString(view.Selector.OrderBy)
		return nil
	}

	return nil
}

func (b *Builder) appendRelationColumn(sb *strings.Builder, aView *view.View, selector *view.Selector, relation *view.Relation) error {
	if relation == nil {
		return nil
	}

	if len(selector.Columns) > 0 {
		return b.checkSelectorAndAppendRelColumn(sb, aView, selector, relation)
	}

	return b.checkViewAndAppendRelColumn(sb, aView, relation)
}

func (b *Builder) checkViewAndAppendRelColumn(sb *strings.Builder, aView *view.View, relation *view.Relation) error {
	if _, ok := aView.ColumnByName(relation.Of.Column); ok {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(aView.Alias)
	sb.WriteString(".")
	sb.WriteString(relation.Of.Column)
	sb.WriteString(" ")

	return nil
}

func (b *Builder) checkSelectorAndAppendRelColumn(sb *strings.Builder, aView *view.View, selector *view.Selector, relation *view.Relation) error {
	if relation == nil || selector.Has(relation.Of.Column) {
		return nil
	}

	sb.WriteString(separatorFragment)
	sb.WriteString(" ")
	col, ok := aView.ColumnByName(relation.Of.Column)
	if !ok {
		sb.WriteString(relation.Of.Column)
	} else {
		sb.WriteString(col.SqlExpression())
	}

	return nil
}

func (b *Builder) getInitialPlaceholders(aView *view.View, selector *view.Selector) ([]interface{}, error) {
	if !aView.UseParamBindingPositions() {
		return make([]interface{}, 0), nil
	}

	totalLen := 0
	for _, parameter := range aView.Template.Parameters {
		totalLen += len(parameter.Positions)
	}

	placeholders := make([]interface{}, totalLen)
	for _, parameter := range aView.Template.Parameters {
		value, err := parameter.Value(selector.Parameters.Values)
		if err != nil {
			return nil, err
		}

		for _, position := range parameter.Positions {
			placeholders[position] = value
		}
	}

	return placeholders, nil
}

func extractSelectorName(name string) string {
	i := 1 // all names starts with the '$'

	for ; i < len(name) && name[i] == '{'; i++ {
	} // skip the select block i.e. ${foo.DbName}

	return name[i : len(name)-i+1]
}
