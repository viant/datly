package reader

import (
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/shared"
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
	whereFragment       = " WHERE "
	inFragment          = " IN ("
	andFragment         = " AND ("
	placeholderFragment = "?"
	encloseFragment     = ")"
)

type (

	//Builder represent SQL Builder
	Builder struct{}

	//BatchData groups data needed to use various data.MatchStrategy
	BatchData struct {
		BatchReadSize int
		CurrentlyRead int
		ColumnName    string
		Values        []interface{}
	}
)

//NewBuilder creates Builder instance
func NewBuilder() *Builder {
	return &Builder{}
}

//Build builds SQL Select statement
func (b *Builder) Build(view *data.View, selector *data.Selector, batchData *BatchData) (string, error) {
	sb := strings.Builder{}
	sb.WriteString(selectFragment)

	var err error
	if err = b.addColumns(view, selector, &sb); err != nil {
		return "", err
	}

	sb.WriteString(fromFragment)
	sb.WriteString(view.Source())
	if view.Alias != "" {
		sb.WriteString(asFragment)
		sb.WriteString(view.Alias)
	}

	hasCriteria := false
	whereFragmentAdded := false
	shouldPutColumnsToSource := false

	if batchData.ColumnName != "" {
		if shouldPutColumnsToSource = strings.Contains(view.Source(), string(shared.ColumnInPosition)); !shouldPutColumnsToSource {
			whereFragmentAdded = true
			sb.WriteString(whereFragment)
			sb.WriteString(b.buildColumnsIn(batchData, view.Alias+"."))
		}
	}

	if view.Criteria != nil {
		if !whereFragmentAdded {
			sb.WriteString(whereFragment)
			whereFragmentAdded = true
		}
		sb.WriteString(view.Criteria.Expression)
		hasCriteria = true
	}

	if view.CanUseClientCriteria() && selector != nil && selector.Criteria != nil {
		if hasCriteria {
			sb.WriteString(andFragment)
			sb.WriteString(selector.Criteria.Expression)
			sb.WriteString(encloseFragment)
		} else {
			if !whereFragmentAdded {
				sb.WriteString(whereFragment)
				whereFragmentAdded = true
			}
			sb.WriteString(selector.Criteria.Expression)
		}
	}

	if err = b.addOrderBy(view, selector, &sb); err != nil {
		return "", err
	}

	b.addLimit(view, selector, batchData, &sb)
	b.addOffset(view, selector, batchData, &sb)

	result := sb.String()
	if shouldPutColumnsToSource {
		columnsIn := b.buildColumnsIn(batchData, "")
		result = strings.Replace(result, string(shared.ColumnInPosition), columnsIn, 1)
	}

	return result, nil
}

func (b *Builder) addColumns(view *data.View, selector *data.Selector, sb *strings.Builder) error {
	columns, err := view.SelectedColumns(selector)
	if err != nil {
		return err
	}
	for i, col := range columns {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(col.SqlExpression())
	}
	return nil
}

func (b *Builder) addOrderBy(view *data.View, selector *data.Selector, sb *strings.Builder) error {
	orderBy := view.Selector.OrderBy
	if view.CanUseClientOrderBy() && selector != nil && selector.OrderBy != "" {
		orderBy = selector.OrderBy
	}

	if orderBy != "" {
		if _, ok := view.ColumnByName(orderBy); !ok {
			return fmt.Errorf("invalid orderBy column: %v", orderBy)
		}
		sb.WriteString(orderByFragment)
		sb.WriteString(orderBy)
	}
	return nil
}

func (b *Builder) addLimit(view *data.View, selector *data.Selector, batchData *BatchData, sb *strings.Builder) {
	limit := view.LimitWithSelector(selector)
	if limit == 0 {
		limit = batchData.BatchReadSize
	} else if limit != 0 {
		toRead := limit - batchData.BatchReadSize - batchData.CurrentlyRead
		if toRead >= 0 {
			limit = batchData.BatchReadSize
		} else if toRead < 0 {
			limit = batchData.BatchReadSize + toRead
		}
	}

	if limit > 0 {
		sb.WriteString(limitFragment)
		sb.WriteString(strconv.Itoa(limit))
	}
}

func (b *Builder) addOffset(view *data.View, selector *data.Selector, batchData *BatchData, sb *strings.Builder) {
	offset := 0
	if selector != nil {

		if view.CanUseClientOffset() && selector.Offset > 0 {
			offset = selector.Offset
		}
	}

	offset += batchData.CurrentlyRead
	if offset > 0 {
		sb.WriteString(offsetFragment)
		sb.WriteString(strconv.Itoa(offset))
	}
}

func (b *Builder) buildColumnsIn(batchData *BatchData, alias string) string {
	sb := strings.Builder{}
	sb.WriteString(alias)
	sb.WriteString(batchData.ColumnName)
	sb.WriteString(inFragment)
	for i := range batchData.Values {
		if i != 0 {
			sb.WriteString(separatorFragment)
		}
		sb.WriteString(placeholderFragment)
	}
	sb.WriteString(") ")
	return sb.String()
}
