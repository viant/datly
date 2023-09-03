package json

type (
	Filters struct {
		fields map[string]Filter
	}

	FilterEntry struct {
		Path   string
		Fields []string
	}

	Filter map[string]bool
)

func NewFilters(filterable ...*FilterEntry) *Filters {
	filters := &Filters{}
	filters.fields = map[string]Filter{}
	for i := range filterable {
		filters.fields[filterable[i].Path] = NewFilter(filterable[i].Fields...)
	}

	return filters
}

func NewFilter(fields ...string) Filter {
	filter := Filter{}
	for i := range fields {
		filter[fields[i]] = true
	}

	return filter
}
