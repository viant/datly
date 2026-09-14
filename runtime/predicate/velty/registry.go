package velty

import (
	"fmt"
	"strings"

	xpredicate "github.com/viant/xdatly/predicate"
)

const (
	predicateEqual             = "equal"
	predicateNotEqual          = "not_equal"
	predicateIn                = "in"
	predicateMultiIn           = "multi_in"
	predicateNotIn             = "not_in"
	predicateMultiNotIn        = "multi_not_in"
	predicateLessOrEqual       = "less_or_equal"
	predicateLessThan          = "less_than"
	predicateGreaterOrEqual    = "greater_or_equal"
	predicateGreaterThan       = "greater_than"
	predicateLike              = "like"
	predicateNotLike           = "not_like"
	predicateHandler           = "handler"
	predicateContains          = "contains"
	predicateNotContains       = "not_contains"
	predicateIsNotNull         = "is_not_null"
	predicateIsNull            = "is_null"
	predicateExists            = "exists"
	predicateNotExists         = "not_exists"
	predicateLiteralIn         = "literal_in"
	predicateExpr              = "expr"
	predicateCriteriaExists    = "exists_criteria"
	predicateCriteriaNotExists = "not_exists_criteria"
	predicateCriteriaIn        = "in_criteria"
	predicateCriteriaNotIn     = "not_in_criteria"
	predicateBetween           = "between"
	predicateDuration          = "duration"
	predicateWhenPresent       = "when_present"
	predicateWhenNotPresent    = "when_not_present"
)

type registryEntry struct {
	template *xpredicate.Template
	handler  bool
}

func predicateRegistry() map[string]registryEntry {
	result := map[string]registryEntry{
		predicateEqual:             {template: binaryTemplate(predicateEqual, "=")},
		predicateNotEqual:          {template: binaryTemplate(predicateNotEqual, "!=")},
		predicateIn:                {template: inTemplate(predicateIn, false, true, false)},
		predicateMultiIn:           {template: inTemplate(predicateMultiIn, false, true, true)},
		predicateNotIn:             {template: inTemplate(predicateNotIn, false, false, false)},
		predicateMultiNotIn:        {template: inTemplate(predicateMultiNotIn, false, false, true)},
		predicateLessOrEqual:       {template: binaryTemplate(predicateLessOrEqual, "<=")},
		predicateLessThan:          {template: binaryTemplate(predicateLessThan, "<")},
		predicateGreaterOrEqual:    {template: binaryTemplate(predicateGreaterOrEqual, ">=")},
		predicateGreaterThan:       {template: binaryTemplate(predicateGreaterThan, ">")},
		predicateLike:              {template: likeTemplate(predicateLike, true)},
		predicateNotLike:           {template: likeTemplate(predicateNotLike, false)},
		predicateContains:          {template: containsTemplate(predicateContains, true)},
		predicateNotContains:       {template: containsTemplate(predicateNotContains, false)},
		predicateIsNull:            {template: isNullTemplate(predicateIsNull, false)},
		predicateIsNotNull:         {template: isNullTemplate(predicateIsNotNull, true)},
		predicateExists:            {template: existsTemplate(predicateExists, false, false)},
		predicateNotExists:         {template: existsTemplate(predicateNotExists, false, true)},
		predicateCriteriaExists:    {template: existsTemplate(predicateCriteriaExists, true, false)},
		predicateCriteriaNotExists: {template: existsTemplate(predicateCriteriaNotExists, true, true)},
		predicateLiteralIn:         {template: literalInTemplate()},
		predicateExpr:              {template: exprTemplate()},
		predicateCriteriaIn:        {template: inTemplate(predicateCriteriaIn, true, true, false)},
		predicateCriteriaNotIn:     {template: inTemplate(predicateCriteriaNotIn, true, false, false)},
		predicateBetween:           {template: betweenTemplate()},
		predicateDuration:          {template: durationTemplate()},
		predicateWhenPresent:       {template: whenPredicateTemplate(predicateWhenPresent, true)},
		predicateWhenNotPresent:    {template: whenPredicateTemplate(predicateWhenNotPresent, false)},
		predicateHandler:           {handler: true},
	}
	templates, closer := xpredicate.Templates(nil)
	if closer != nil {
		defer closer()
	}
	for name, template := range templates {
		if template == nil {
			continue
		}
		result[strings.ToLower(name)] = registryEntry{template: template}
	}
	return result
}

func binaryTemplate(name, operator string) *xpredicate.Template {
	return &xpredicate.Template{
		Name:   name,
		Source: " ${Alias}.${ColumnName} " + operator + " $criteria.AppendBinding($FilterValue)",
		Args: []*xpredicate.NamedArgument{
			{Name: "Alias", Position: 0},
			{Name: "ColumnName", Position: 1},
		},
	}
}

func inTemplate(name string, withCriteria bool, inclusive bool, multi bool) *xpredicate.Template {
	args := []*xpredicate.NamedArgument{
		{Name: "Alias", Position: 0},
	}
	column := `${Alias}`
	if !multi {
		column += `+ "." + ${ColumnName}`
		args = append(args, &xpredicate.NamedArgument{Name: "ColumnName", Position: 1})
		if withCriteria {
			args = append(args,
				&xpredicate.NamedArgument{Name: "LookupAlias", Position: 2},
				&xpredicate.NamedArgument{Name: "LookupTable", Position: 3},
				&xpredicate.NamedArgument{Name: "LookupColumn", Position: 4},
				&xpredicate.NamedArgument{Name: "FilterColumn", Position: 5},
				&xpredicate.NamedArgument{Name: "Criterion", Position: 6},
			)
		}
	}
	in := fmt.Sprintf(`$criteria.In(%v, $FilterValue)`, column)
	if !inclusive {
		in = fmt.Sprintf(`$criteria.NotIn(%v, $FilterValue)`, column)
	}
	if withCriteria {
		in = `${Alias}.${ColumnName} IN (SELECT ${LookupAlias}.${LookupColumn} FROM ${LookupTable} ${LookupAlias} ` +
			`WHERE ${Criterion} AND $criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))`
		if !inclusive {
			in = `${Alias}.${ColumnName} NOT IN (SELECT ${LookupAlias}.${LookupColumn} FROM ${LookupTable} ${LookupAlias} ` +
				`WHERE ${Criterion} AND $criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))`
		}
	}
	return &xpredicate.Template{Name: name, Source: " " + in, Args: args}
}

func likeTemplate(name string, inclusive bool) *xpredicate.Template {
	column := `${Alias}` + `+ "." + ${ColumnName}`
	criteria := fmt.Sprintf(`$criteria.Like(%v, $FilterValue)`, column)
	if !inclusive {
		criteria = fmt.Sprintf(`$criteria.NotLike(%v, $FilterValue)`, column)
	}
	return &xpredicate.Template{
		Name:   name,
		Source: " " + criteria,
		Args: []*xpredicate.NamedArgument{
			{Name: "Alias", Position: 0},
			{Name: "ColumnName", Position: 1},
		},
	}
}

func containsTemplate(name string, inclusive bool) *xpredicate.Template {
	column := `${Alias}` + `+ "." + ${ColumnName}`
	criteria := fmt.Sprintf(`$criteria.Contains(%v, $FilterValue)`, column)
	if !inclusive {
		criteria = fmt.Sprintf(`$criteria.NotContains(%v, $FilterValue)`, column)
	}
	return &xpredicate.Template{
		Name:   name,
		Source: " " + criteria,
		Args: []*xpredicate.NamedArgument{
			{Name: "Alias", Position: 0},
			{Name: "ColumnName", Position: 1},
		},
	}
}

func isNullTemplate(name string, negated bool) *xpredicate.Template {
	clause := `${Alias}.${Column} IS NULL`
	if negated {
		clause = `${Alias}.${Column} IS NOT NULL`
	}
	return &xpredicate.Template{
		Name:   name,
		Source: " " + clause,
		Args: []*xpredicate.NamedArgument{
			{Name: "Alias", Position: 0},
			{Name: "Column", Position: 1},
		},
	}
}

func existsTemplate(name string, withCriteria bool, negated bool) *xpredicate.Template {
	args := []*xpredicate.NamedArgument{
		{Name: "Alias", Position: 0},
		{Name: "Column", Position: 1},
		{Name: "LookupAlias", Position: 2},
		{Name: "LookupTable", Position: 3},
		{Name: "LookupColumn", Position: 4},
		{Name: "FilterColumn", Position: 5},
	}
	if withCriteria {
		args = append(args, &xpredicate.NamedArgument{Name: "Criterion", Position: 6})
	}
	clause := ` EXISTS (SELECT 1 FROM ${LookupTable} ${LookupAlias} ` +
		`WHERE ${LookupAlias}.${LookupColumn} = ${Alias}.${Column} AND ` +
		`$criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))`
	if withCriteria {
		clause = ` EXISTS (SELECT 1 FROM ${LookupTable} ${LookupAlias} ` +
			`WHERE ${LookupAlias}.${LookupColumn} = ${Alias}.${Column} AND ${Criterion} AND ` +
			`$criteria.In(${LookupAlias} + "." + ${FilterColumn}, $FilterValue))`
	}
	if negated {
		clause = " NOT " + clause
	}
	return &xpredicate.Template{Name: name, Source: " " + clause, Args: args}
}

func literalInTemplate() *xpredicate.Template {
	return &xpredicate.Template{
		Name:   predicateLiteralIn,
		Source: ` $criteria.In($Literal, $FilterValue)`,
		Args:   []*xpredicate.NamedArgument{{Name: "Literal", Position: 0}},
	}
}

func exprTemplate() *xpredicate.Template {
	return &xpredicate.Template{
		Name:   predicateExpr,
		Source: ` $criteria.Expression($Expression, $FilterValue)`,
		Args:   []*xpredicate.NamedArgument{{Name: "Expression", Position: 0}},
	}
}

func betweenTemplate() *xpredicate.Template {
	return &xpredicate.Template{
		Name: predicateBetween,
		Source: ` ${Expression} BETWEEN
      #if($FilterValue.Has.ValueMin)
          $criteria.AppendBinding($FilterValue.ValueMin)
      #else
          NULL
      #end
      AND
      #if($FilterValue.Has.ValueMax)
          $criteria.AppendBinding($FilterValue.ValueMax)
      #else
          NULL
      #end`,
		Args: []*xpredicate.NamedArgument{
			{Name: "Expression", Position: 0},
			{Name: "From", Position: 1},
			{Name: "To", Position: 2},
		},
	}
}

func durationTemplate() *xpredicate.Template {
	return &xpredicate.Template{
		Name: predicateDuration,
		Source: `
#if($FilterValue == "hour")
	   ${DayExpression} = ${CurrentDayExpression}
	  AND ${HourExpression} = ${CurrentHourExpression}
#elseif($FilterValue == "HOUR")
	   ${DayExpression} = ${CurrentDayExpression}
	  AND ${HourExpression} = ${CurrentHourExpression}
#elseif($FilterValue == "day")
	 ${DayExpression} = ${CurrentDayExpression}
#elseif($FilterValue == "DAY")
	 ${DayExpression} = ${CurrentDayExpression}
#elseif($FilterValue == "today")
	 ${DayExpression} = ${CurrentDayExpression}
#elseif($FilterValue == "TODAY")
	 ${DayExpression} = ${CurrentDayExpression}
#elseif($FilterValue == "yesterday")
 	 ${DayExpression} = ${YesterdayDayExpression}
#elseif($FilterValue == "YESTERDAY")
 	 ${DayExpression} = ${YesterdayDayExpression}
#elseif($FilterValue == "week")
 	 ${DayExpression} BETWEEN ${WeekDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "WEEK")
 	 ${DayExpression} BETWEEN ${WeekDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "seven_days")
 	 ${DayExpression} BETWEEN ${WeekDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "SEVEN_DAYS")
 	 ${DayExpression} BETWEEN ${WeekDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "month")
 	 ${DayExpression} BETWEEN ${MonthDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "MONTH")
 	 ${DayExpression} BETWEEN ${MonthDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "thirty_days")
 	 ${DayExpression} BETWEEN ${MonthDayExpression}  AND ${CurrentDayExpression}
#elseif($FilterValue == "THIRTY_DAYS")
 	 ${DayExpression} BETWEEN ${MonthDayExpression}  AND ${CurrentDayExpression}
#end
`,
		Args: []*xpredicate.NamedArgument{
			{Name: "DayExpression", Position: 0},
			{Name: "CurrentDayExpression", Position: 1},
			{Name: "HourExpression", Position: 2},
			{Name: "CurrentHourExpression", Position: 3},
			{Name: "YesterdayDayExpression", Position: 4},
			{Name: "WeekDayExpression", Position: 5},
			{Name: "MonthDayExpression", Position: 6},
		},
	}
}

func whenPredicateTemplate(name string, present bool) *xpredicate.Template {
	condition := `#if($HasFilterValue) ${Criterion} #end`
	if !present {
		condition = `#if(!$HasFilterValue) ${Criterion} #end`
	}
	return &xpredicate.Template{
		Name:   name,
		Source: " " + condition,
		Args:   []*xpredicate.NamedArgument{{Name: "Criterion", Position: 0}},
	}
}
