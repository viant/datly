package keywords

import "github.com/viant/velty/functions"

var (
	ParamsMetadataKey = AddAndGet("Has",
		functions.NewEntry(
			nil,
			NewNamespace(),
		))
	ParamsKey = AddAndGet("Unsafe", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	KeySQL = "sql"

	Pagination = AddAndGet("$PAGINATION", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	Criteria = AddAndGet("$CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	WhereCriteria = AddAndGet("$WHERE_CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	ColumnsIn = AddAndGet("$COLUMN_IN", functions.NewEntry(
		nil,
		NewNamespace(),
	))
	AndColumnInPosition = AddAndGet("$AND_COLUMN_IN", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	SelectorCriteria = AddAndGet("$SELECTOR_CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))
	WhereSelectorCriteria = AddAndGet("$WHERE_SELECTOR_CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	AndSelectorCriteria = AddAndGet("$AND_SELECTOR_CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))
	AndCriteria = AddAndGet("$AND_CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))
	OrCriteria = AddAndGet("$OR_CRITERIA", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	Rec = AddAndGet("$Rec", functions.NewEntry(
		nil,
		NewNamespace(),
	))

	WherePrefix = "WHERE_"
	AndPrefix   = "AND_"
	OrPrefix    = "OR_"
)
