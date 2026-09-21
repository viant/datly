package compiler

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

// currentProjection compiles declared scalar assignments only. Runtime schema
// evidence determines which of these assignments applies to an actual row.
type currentProjection struct {
	entity, current *spec.View
}

func (p *currentProjection) compile() ([]plan.CurrentField, error) {
	var result []plan.CurrentField
	used := make(map[string]string)
	for _, entity := range p.entity.Columns {
		if entity == nil {
			continue
		}
		current, err := p.match(entity)
		if err != nil {
			return nil, err
		}
		if current == nil {
			continue
		}
		from, to := p.field(current), p.field(entity)
		if from.Field == "" || to.Field == "" {
			return nil, fmt.Errorf("current view %q projection requires canonical Go field names", p.current.CanonicalName())
		}
		if prior, ok := used[to.Field]; ok {
			return nil, fmt.Errorf("current view %q has duplicate entity field %q (sources %q and %q)", p.current.CanonicalName(), to.Field, prior, to.Source)
		}
		used[to.Field] = to.Source
		if !sameType(from.Type, to.Type) || (from.Type.Cardinality == spec.CardinalityMany) != (to.Type.Cardinality == spec.CardinalityMany) {
			return nil, fmt.Errorf("current view %q field %q type %+v cannot project into entity field %q type %+v", p.current.CanonicalName(), from.Field, from.Type, to.Field, to.Type)
		}
		result = append(result, plan.CurrentField{Current: from, Entity: to, Conversion: linkConversion(from.Type, to.Type)})
	}
	return result, nil
}

func (p *currentProjection) match(entity *spec.Column) (*spec.Column, error) {
	field, source := typecatalog.FieldName(entity.Name), strings.TrimSpace(entity.Source)
	var result *spec.Column
	for _, current := range p.current.Columns {
		if current == nil {
			continue
		}
		fieldMatch := typecatalog.FieldName(current.Name) == field
		currentSource := currentColumnOrigin(current)
		sourceMatch := source != "" && strings.EqualFold(source, currentSource)
		if fieldMatch && source != "" && currentSource != "" && !sourceMatch {
			return nil, fmt.Errorf("current view %q field %q source %q conflicts with entity source %q", p.current.CanonicalName(), current.Name, currentSource, source)
		}
		if !fieldMatch && !sourceMatch {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("current view %q has ambiguous entity field %q", p.current.CanonicalName(), field)
		}
		result = current
	}
	return result, nil
}

func (p *currentProjection) field(column *spec.Column) plan.FieldRef {
	return plan.FieldRef{Field: typecatalog.FieldName(column.Name), Source: currentColumnOrigin(column), Type: column.EffectiveType()}
}

func currentColumnOrigin(column *spec.Column) string {
	if column == nil {
		return ""
	}
	if origin := strings.TrimSpace(column.Expression); column.PrimaryKey && origin != "" {
		return origin
	}
	return strings.TrimSpace(column.Source)
}
