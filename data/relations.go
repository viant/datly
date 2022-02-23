package data

import "github.com/viant/datly/shared"

//RelationsSlice represents slice of Relation
type RelationsSlice []*Relation

//Index indexes Relations by Relation.Holder
//Uses shared.KeysOf
func (r RelationsSlice) Index() map[string]*Relation {
	result := make(map[string]*Relation)
	for i, rel := range r {
		keys := shared.KeysOf(rel.Holder, true)

		for _, key := range keys {
			result[key] = r[i]
		}
	}

	return result
}

//PopulateWithResolve filters RelationsSlice by the columns that won't be present in Database
//due to the removing StructField after assembling nested StructType.
func (r RelationsSlice) PopulateWithResolve() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if !rel.hasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}

//Columns extract Relation.Column from RelationsSlice
func (r RelationsSlice) Columns() []string {
	resolverColumns := make([]string, 0)
	for i := range r {
		resolverColumns = append(resolverColumns, r[i].Column)
	}
	return resolverColumns
}

//PopulateWithVisitor filters RelationsSlice by the columns that will be present in Database, and because of that
//they wouldn't be resolved as unmapped columns. 
func (r RelationsSlice) PopulateWithVisitor() []*Relation {
	result := make([]*Relation, 0)
	for i, rel := range r {
		if rel.hasColumnField {
			result = append(result, r[i])
		}
	}

	return result
}
