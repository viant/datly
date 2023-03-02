package keywords

var (
	ParamsMetadataKey = ReservedKeywords.AddAndGet("Has")
	ParamsKey         = ReservedKeywords.AddAndGet("Unsafe")
	KeyView           = ReservedKeywords.AddAndGet("View")
	KeySQL            = ReservedKeywords.AddAndGet("sql")
	KeyParentView     = ReservedKeywords.AddAndGet("ParentView")
	KeySequencer      = ReservedKeywords.AddAndGet("sequencer")
	KeySQLx           = ReservedKeywords.AddAndGet("sqlx")

	Pagination    = ReservedKeywords.AddAndGet("$PAGINATION")
	Criteria      = ReservedKeywords.AddAndGet("$CRITERIA")
	WhereCriteria = ReservedKeywords.AddAndGet("$WHERE_CRITERIA")

	ColumnsIn           = ReservedKeywords.AddAndGet("$COLUMN_IN")
	AndColumnInPosition = ReservedKeywords.AddAndGet("$AND_COLUMN_IN")

	SelectorCriteria      = ReservedKeywords.AddAndGet("$SELECTOR_CRITERIA")
	WhereSelectorCriteria = ReservedKeywords.AddAndGet("$WHERE_SELECTOR_CRITERIA")
	AndSelectorCriteria   = ReservedKeywords.AddAndGet("$AND_SELECTOR_CRITERIA")
	AndCriteria           = ReservedKeywords.AddAndGet("$AND_CRITERIA")
	OrCriteria            = ReservedKeywords.AddAndGet("$OR_CRITERIA")

	WherePrefix = "WHERE_"
	AndPrefix   = "AND_"
	OrPrefix    = "OR_"
)
