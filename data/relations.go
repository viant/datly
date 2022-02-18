package data

//RelationsSlice represents slice of Relation
type RelationsSlice []*Relation

//Index indexes Relations by Relation.Holder
func (r RelationsSlice) Index() map[string]*Relation {
	result := make(map[string]*Relation)
	for i, rel := range r {
		keys := KeysOf(rel.Holder, true)

		for _, key := range keys {
			result[key] = r[i]
		}
	}

	return result
}

func (r RelationsSlice) PopulateWithResolve() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if !rel.HasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}

func (r RelationsSlice) Columns() []string {
	resolverColumns := make([]string, 0)
	for i := range r {
		resolverColumns = append(resolverColumns, r[i].Column)
	}
	return resolverColumns
}

func (r RelationsSlice) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if rel.HasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}
