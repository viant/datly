package column

import (
	"fmt"
	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
	"reflect"
)

// Prepared physical facts use the same enrichment as database discovery, but
// only after native SQL lineage proves the result's physical column identity.
func preparedTableConstraints(columns []*spec.Column, lineage *projectionLineage) (map[string]tableConstraint, error) {
	result := map[string]tableConstraint{}
	for _, column := range columns {
		name := normalizedName(column.Name)
		physical, found := lineage.direct[name]
		if !found && lineage.wildcard && !lineage.blocked[name] {
			physical, found = name, name != ""
		}
		if !found {
			continue
		} // Computed/joined results have no authority in this table.
		supplied := firstValue(column.Source, column.Name)
		if normalizedName(supplied) != physical {
			if normalizedName(supplied) == name {
				continue
			} // Result-only metadata claims no physical alias.
			return nil, fmt.Errorf("prepared source %s disagrees with SQL lineage for %s", supplied, column.Name)
		}
		fact := tableConstraint{name: supplied, primaryKey: column.PrimaryKey, autoIncrement: column.AutoIncrement, unique: column.Unique, notNull: column.NotNull, defaultValue: column.Default}
		tag := sqlio.ParseTag(reflect.StructTag(column.Tag))
		if tag != nil && tag.RefTable != "" && tag.RefColumn != "" {
			fact.reference = &tableReference{schema: tag.RefDb, table: tag.RefTable, column: tag.RefColumn}
		}
		if prior, exists := result[physical]; exists && !reflect.DeepEqual(prior, fact) {
			return nil, fmt.Errorf("conflicting prepared physical facts for %s", supplied)
		}
		result[physical] = fact
	}
	return result, nil
}
