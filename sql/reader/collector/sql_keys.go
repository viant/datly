package collector

// SQLKeyColumns includes keys for child reads and this view's attachment to
// its parent. Mapped columns need snapshots too, independent of row hooks.
func (r *Collector) SQLKeyColumns() map[string]bool {
	result := map[string]bool{}
	add := func(links Links) {
		for _, link := range links {
			if link != nil && link.XField == nil {
				result[link.Column] = true
			}
		}
	}
	for _, relation := range r.view.Relations {
		if relation != nil {
			add(relation.On)
		}
	}
	if r.relation != nil && r.relation.Of != nil {
		add(r.relation.Of.On)
	}
	return result
}

// ReserveSQLKey follows the unmapped allocator's row lifecycle, including
// rollback when a cache matcher skips a row. Fill the slot before row hooks.
func (r *Collector) ReserveSQLKey(column string) *any {
	buffer := r.values[column]
	if buffer == nil {
		values := make([]any, 0)
		buffer = &values
		r.values[column] = buffer
	}
	value := new(any)
	*buffer = append(*buffer, value)
	return value
}
