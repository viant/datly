package generate

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/tagly/tags"
	xshape "github.com/viant/x/shape"
)

// Ordinary ownership is independent of generated factory InputShape semantics.
// Early input materialization may precede native operation selection, but only
// the existing declared transient delete metadata permits that staging.
func (r *planResolver) ordinaryBodyRoot() *spec.View {
	c := r.input.Component
	if c == nil || c.RootView == nil || r.input.ExternalHandler != nil || r.input.VeltyHandler != nil || r.input.GoHandler != nil || r.input.ContractHandler != nil || r.input.Views[RootViewPath] != nil {
		return nil
	}
	root := c.RootView
	if root.InMemory || strings.TrimSpace(root.TypeName) == "" {
		return nil
	}
	if !r.input.nativeMutationTargetSelected && !ordinaryBodyDeleteMarker(root) {
		return nil
	}
	canonical, err := r.plan.CanonicalType(r.plan.Package, root.TypeName)
	if err != nil {
		return nil
	}
	planned, err := r.plan.CanonicalType(r.plan.Package, r.plan.RootViewType)
	if err != nil || planned != canonical {
		return nil
	}
	matched := false
	for _, param := range preferDefinedParams(c.Parameters) {
		if param == nil {
			continue
		}
		kind := strings.ToLower(strings.TrimSpace(param.Source.Kind))
		if kind != "body" && !(kind == "output" && param.Source.Name == "body") {
			continue
		}
		expression := strings.TrimSpace(param.TypeExpr)
		if expression == "" || expression == "?" {
			continue
		}
		actual, err := r.plan.CanonicalType(r.plan.Package, expression)
		if err != nil {
			return nil
		}
		named, err := (xshape.Resolver{}).Named(actual)
		if err != nil || named != canonical {
			return nil
		}
		matched = true
	}
	if !matched {
		return nil
	}
	return root
}

type ordinaryBodyColumn struct {
	column *spec.Column
	field  Field
}

func (p *viewPlanner) shareOrdinaryBodyRow(current, owner *spec.View, destination string) error {
	fail := func(reason string) error {
		return fmt.Errorf("ordinary shared body %q read %q: %s", owner.Name, current.Name, reason)
	}
	if p.requireConcrete && !p.nativeMutation {
		return fail("requires normalized native mutation selection")
	}
	if !p.requireConcrete && !p.nativeMutation && !HasWritableDeleteMarker(owner) && !p.ordinaryGraph[owner] {
		return fail("provisional owner requires native delete metadata")
	}
	if owner.SelfReference != nil || (len(owner.Relations) != 0 && !p.ordinaryGraph[owner]) {
		return fail("body must remain an independent leaf")
	}
	if current.Auxiliary || current.InMemory || len(current.Relations) != 0 || current.SelfReference != nil {
		return fail("read must remain an independent leaf")
	}
	if owner.Source == nil || current.Source == nil || strings.TrimSpace(owner.Source.Table) == "" || !strings.EqualFold(owner.Source.Table, current.Source.Table) {
		return fail("table compatibility does not match")
	}
	connector := func(v *spec.View) string {
		if v.Source.Bindings != nil && strings.TrimSpace(v.Source.Bindings.Connector) != "" {
			return v.Source.Bindings.Connector
		}
		return p.plan.Connector
	}
	if connector(owner) == "" || connector(owner) != connector(current) {
		return fail("connector compatibility does not match")
	}
	business, err := p.ordinaryColumns(owner, true)
	if err != nil {
		return fail(err.Error())
	}
	// The existing discovery input is materialized before independent-read
	// columns are refined. Stage only its type/destination edge here; the same
	// precheck runs with complete columns and concrete target before emission.
	if !p.requireConcrete {
		return p.shareBodyRow(current, owner, destination)
	}
	reads, err := p.ordinaryColumns(current, false)
	if err != nil {
		return fail(err.Error())
	}
	if len(business) != len(reads) {
		return fail("business column coverage does not match")
	}
	for physical, read := range reads {
		body, ok := business[physical]
		if !ok {
			return fail("read column has no exact body mapping: " + physical)
		}
		if p.requireConcrete {
			bodyType, err := p.plan.CanonicalType(p.plan.Package, body.field.Type)
			if err != nil {
				return err
			}
			readType, err := p.plan.CanonicalType(p.plan.Package, read.field.Type)
			if err != nil {
				return err
			}
			if bodyType != readType {
				return fail("canonical scalar type mismatch: " + physical + ": " + bodyType + " versus " + readType)
			}
		}
		if read.field.Name != body.field.Name {
			return fail("public field identity does not match: " + physical)
		}
		if !ordinaryBodyTags(body, read) {
			return fail("field tags conflict: " + physical)
		}
	}
	// Final ordinary types are already exact. Reuse existing destination and
	// mapping checks, without granting its inferred/refined mismatch exception.
	// Each independent read retains its own SQL, including computed projections.
	return p.shareBodyRow(current, owner, destination)
}

func (p *viewPlanner) ordinaryColumns(view *spec.View, body bool) (map[string]ordinaryBodyColumn, error) {
	fields, err := p.fields(view)
	if err != nil {
		return nil, err
	}
	byName := map[string]Field{}
	for _, field := range fields {
		byName[field.Name] = field
	}
	result := map[string]ordinaryBodyColumn{}
	for _, column := range view.Columns {
		if column == nil {
			return nil, fmt.Errorf("incomplete scalar column")
		}
		field, ok := byName[typecatalog.FieldName(column.Name)]
		if !ok || field.RelationHolder {
			return nil, fmt.Errorf("missing scalar field authority")
		}
		mapping := sqlio.ParseTag(reflect.StructTag(field.Tag))
		if mapping.Transient {
			tag := reflect.StructTag(field.Tag)
			if body && column.DeleteMarker && tag.Get("writer") == "delete" && tag.Get("internal") == "true" && tag.Get("json") == "-" && tag.Get("diff") == "-" {
				continue
			}
			return nil, fmt.Errorf("unclassified transient column %q", column.Name)
		}
		source := strings.TrimSpace(column.Source)
		parts, err := sqlparser.TableIdentifierParts(source)
		if source == "" || err != nil || len(parts) != 1 {
			return nil, fmt.Errorf("missing column mapping %q", column.Name)
		}
		physical := strings.ToLower(parts[0])
		mapped := strings.Split(mapping.Column, "|")
		if len(mapped) == 0 || !strings.EqualFold(mapped[0], parts[0]) {
			return nil, fmt.Errorf("SQLX/source mapping conflicts for %q", column.Name)
		}
		if _, duplicate := result[physical]; duplicate {
			return nil, fmt.Errorf("ambiguous column mapping %q", source)
		}
		result[physical] = ordinaryBodyColumn{column: column, field: field}
	}
	return result, nil
}

// Body validation/presence is owned by the writer. Only exact native internal
// visibility may differ from inferred read defaults; explicit read policy and
// unknown semantic tag differences cannot silently disappear. Only native
// inferred omitempty may differ when the read has no authored JSON policy.
func ordinaryBodyTags(body, read ordinaryBodyColumn) bool {
	rawBody, rawRead := reflect.StructTag(body.column.Tag), reflect.StructTag(read.column.Tag)
	if value, ok := rawRead.Lookup("sqlx"); ok && value != rawBody.Get("sqlx") {
		return false
	}
	if value, ok := rawRead.Lookup("validate"); ok && value != rawBody.Get("validate") {
		return false
	}
	values := func(raw string) map[string]string {
		result := map[string]string{}
		for _, item := range tags.NewTags(raw) {
			if item != nil {
				result[item.Name] = string(item.Values)
			}
		}
		return result
	}
	owner, reader := values(body.field.Tag), values(read.field.Tag)
	delete(owner, "sqlx")
	delete(reader, "sqlx")
	delete(owner, "validate")
	delete(reader, "validate")
	// A shared read uses the body's presentation policy even when its empty
	// name is inferred. Do not turn that read into a body or relax authored tags.
	if value := owner["json"]; strings.Split(value, ",")[0] == "" {
		if _, explicitRead := rawRead.Lookup("json"); !explicitRead {
			parts := strings.Split(reader["json"], ",")
			parts[0] = ""
			normalized := parts[:1]
			for _, option := range parts[1:] {
				if option != "omitempty" {
					normalized = append(normalized, option)
				}
			}
			for _, option := range strings.Split(value, ",")[1:] {
				if option == "omitempty" {
					normalized = append(normalized, option)
				}
			}
			reader["json"] = strings.Join(normalized, ",")
			if _, present := owner["json"]; !present && reader["json"] == "" {
				delete(reader, "json")
			}
		}
	}
	if _, explicitRead := rawRead.Lookup("json"); !explicitRead && owner["json"] != reader["json"] {
		// The one emitted body owns presentation. Compare names/options exactly
		// after removing only the optional inference on the read side.
		parts := strings.Split(reader["json"], ",")
		withoutOmit := parts[:1]
		for _, option := range parts[1:] {
			if option != "omitempty" {
				withoutOmit = append(withoutOmit, option)
			}
		}
		if owner["json"] == strings.Join(withoutOmit, ",") {
			if value, authored := owner["json"]; authored {
				reader["json"] = value
			} else {
				delete(reader, "json")
			}
		}
	}

	if owner["json"] != reader["json"] {
		_, explicitRead := rawRead.Lookup("json")
		if rawBody.Get("internal") != "true" || rawBody.Get("json") != "-" || explicitRead || rawRead.Get("internal") != "" {
			return false
		}
		delete(owner, "json")
		delete(reader, "json")
		delete(owner, "internal")
	}
	return reflect.DeepEqual(owner, reader)
}

// ordinaryBodyDeleteMarker proves only provisional row ownership. Auxiliary
// containers may hold writable descendants; this must not widen DELETE routing.
func ordinaryBodyDeleteMarker(view *spec.View) bool {
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) bool
	visit = func(v *spec.View) bool {
		if v == nil || seen[v] || v.InMemory {
			return false
		}
		seen[v] = true
		if !v.Auxiliary {
			for _, col := range v.Columns {
				if col != nil && col.DeleteMarker {
					return true
				}
			}
		}
		for _, rel := range v.Relations {
			if rel != nil && rel.Kind != spec.RelationKindDerived && visit(rel.View) {
				return true
			}
		}
		return false
	}
	return visit(view)
}

// Only generated nodes in the declared auxiliary body graph may share a row.
// Independent views are deliberately absent from this ownership set.
func ordinaryBodyGraph(root *spec.View) map[*spec.View]bool {
	if root == nil || !root.Auxiliary {
		return nil
	}
	result := map[*spec.View]bool{}
	var visit func(*spec.View)
	visit = func(v *spec.View) {
		if v == nil || result[v] || v.InMemory {
			return
		}
		result[v] = true
		for _, rel := range v.Relations {
			if rel != nil && rel.Kind != spec.RelationKindDerived {
				visit(rel.View)
			}
		}
	}
	visit(root)
	return result
}
