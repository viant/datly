package data

type Relations []*Relation

func (r Relations) Index() map[string]*Relation {
	result := make(map[string]*Relation)
	for i, rel := range r {
		keys := KeysOf(rel.Holder, true)

		for _, key := range keys {
			result[key] = r[i]
		}
	}

	return result
}

func (r Relations) PopulateWithResolve() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if !rel.HasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}

func (r Relations) Columns() []string {
	resolverColumns := make([]string, 0)
	for i := range r {
		resolverColumns = append(resolverColumns, r[i].Column)
	}
	return resolverColumns
}

func (r Relations) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if rel.HasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}
