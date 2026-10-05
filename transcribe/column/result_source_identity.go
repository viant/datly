package column

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	sqlio "github.com/viant/sqlx/io"
)

// resultSourceIdentity is private to discovery. Source normally names a vendor
// result, but table constraint enrichment can replace it with a physical DML
// column. Only proven SQL lineage can recover that vendor identity; an unrelated
// Go field name or an unproven sqlx mapping never supplies result authority.
type resultSourceIdentity map[*spec.Column]string

func resolveResultSources(columns []*spec.Column, source *spec.ViewSource) (resultSourceIdentity, error) {
	lineage, err := directProjectionLineage(source)
	if err != nil {
		return nil, err
	}
	result := make(resultSourceIdentity, len(columns))
	owners := map[string]*spec.Column{}
	for _, column := range columns {
		if column == nil {
			continue
		}
		identity := strings.TrimSpace(column.Source)
		if identity == "" {
			if !column.NameInferred {
				identity = strings.TrimSpace(column.Name)
			}
		} else {
			physical := normalizedName(identity)
			// A dual physical|vendor SQLX mapping retains proven alias provenance,
			// including descriptors whose Go names were inferred from a row type.
			candidates := map[string]string{}
			mapping := sqlio.ParseTag(reflect.StructTag(column.Tag)).Column
			alternatives := strings.Split(mapping, "|")
			_, directResult := lineage.direct[physical]
			sourceExposed := directResult || lineage.blocked[physical] || lineage.wildcard
			dualMapping := len(alternatives) > 1 && normalizedName(alternatives[0]) == physical
			for _, alias := range alternatives {
				key := normalizedName(alias)
				if (!sourceExposed || dualMapping) && key != physical && lineage.direct[key] == physical && !lineage.blocked[key] {
					candidates[key] = lineage.names[key]
				}
			}
			if len(candidates) > 1 {
				return nil, fmt.Errorf("column %q has ambiguous SQL result mappings", column.Name)
			}
			if len(candidates) == 1 {
				for _, name := range candidates {
					identity = name
				}
			} else if _, actual := lineage.direct[physical]; !actual && !lineage.wildcard {
				// A canonical vendor alias remains usable after constraint enrichment,
				// but inferred Go row names do not establish aliases.
				name := normalizedName(column.Name)
				if !column.NameInferred && lineage.direct[name] == physical && !lineage.blocked[name] {
					identity = lineage.names[name]
				}
			}
		}
		key := normalizedName(identity)
		if key != "" {
			if previous := owners[key]; previous != nil {
				return nil, fmt.Errorf("columns %q and %q have ambiguous SQL result ownership for %q", previous.Name, column.Name, identity)
			}
			owners[key] = column
		}
		result[column] = identity
	}
	return result, nil
}

func (r resultSourceIdentity) validateResults(columns []*spec.Column, projected []string) error {
	counts := map[string]int{}
	for _, name := range projected {
		key := normalizedName(name)
		counts[key]++
		if counts[key] > 1 {
			return fmt.Errorf("SQL result column %q matches multiple projected columns", name)
		}
	}
	for _, column := range columns {
		if column == nil {
			continue
		}
		identity := r[column]
		if counts[normalizedName(identity)] == 0 {
			return fmt.Errorf("column %q source %q is absent from the SQL projection", column.Name, identity)
		}
	}
	return nil
}
