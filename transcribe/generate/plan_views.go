package generate

import (
	"fmt"
	"go/ast"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/tagly/format/text"
)

type viewPlanner struct {
	plan     *Plan
	velty    bool
	names    map[*spec.View]string
	owners   map[string]*spec.View
	parents  map[*spec.View]string
	dests    map[*spec.View]string
	visiting map[*spec.View]bool
	ordered  []*spec.View
	outputs  map[*spec.Relation]*spec.Parameter
}

func (r *planResolver) resolveViews() (map[string]int, error) {
	plan := r.plan
	component := r.input.Component
	indexes := map[string]int{}
	if plan == nil || component == nil {
		return indexes, nil
	}
	planner := &viewPlanner{
		plan: plan, velty: r.input.VeltyHandler != nil, names: map[*spec.View]string{}, owners: map[string]*spec.View{},
		parents: map[*spec.View]string{}, dests: map[*spec.View]string{}, visiting: map[*spec.View]bool{},
		outputs: map[*spec.Relation]*spec.Parameter{},
	}
	for _, param := range preferDefinedParams(component.Parameters) {
		if !param.IsDerivedOutput() {
			continue
		}
		relation := outputRelation(component.RootView, param.Name)
		if relation == nil || relation.View == nil || len(relation.On) != 0 {
			return nil, fmt.Errorf("output relation %s requires a derived view without row links", param.Name)
		}
		planner.outputs[relation] = param
	}
	if root := component.RootView; root != nil {
		rootIdentity, err := root.Identity()
		if err != nil {
			return nil, fmt.Errorf("plan root view: %w", err)
		}
		if reference, ok := r.input.Views[RootViewPath]; ok {
			linked, err := r.resolveLinkedView(root, reference, rootIdentity)
			if err != nil {
				return nil, err
			}
			plan.RootViewType = linked.Type
			plan.Views = append(plan.Views, linked)
			for _, relation := range root.Relations {
				if _, output := planner.outputs[relation]; !output {
					continue
				}
				destination, err := generatedViewDestination(relation.View, plan.ViewDest)
				if err != nil {
					return nil, err
				}
				if err := planner.assign(relation.View, generatedChildViewType(relation), destination); err != nil {
					return nil, err
				}
			}
		} else {
			rootDest, err := generatedViewDestination(root, plan.ViewDest)
			if err != nil {
				return nil, err
			}
			plan.ViewDest = rootDest
			if err := planner.assign(root, plan.RootViewType, rootDest); err != nil {
				return nil, err
			}
		}
	}
	generatedRoots := map[*spec.View]string{}
	type linkedIndependentView struct {
		identity string
		plan     ViewPlan
	}
	var linkedViews []linkedIndependentView
	for _, view := range component.Views {
		identity, err := view.Identity()
		if err != nil {
			return nil, fmt.Errorf("plan independent view: %w", err)
		}
		if reference, ok := r.input.Views[identity]; ok {
			linked, err := r.resolveLinkedView(view, reference, identity)
			if err != nil {
				return nil, err
			}
			linkedViews = append(linkedViews, linkedIndependentView{identity: identity, plan: linked})
			continue
		}
		typeName, err := r.independentViewType(view, identity)
		if err != nil {
			return nil, err
		}
		destination, err := generatedViewDestination(view, plan.ViewDest)
		if err != nil {
			return nil, err
		}
		if err := planner.assign(view, typeName, destination); err != nil {
			return nil, err
		}
		generatedRoots[view] = identity
	}
	for _, view := range planner.ordered {
		fields, err := planner.fields(view)
		if err != nil {
			return nil, err
		}
		name := planner.names[view]
		identity, err := view.Identity()
		if err != nil {
			return nil, fmt.Errorf("plan generated view %q: %w", name, err)
		}
		if identity := generatedRoots[view]; identity != "" {
			indexes[identity] = len(plan.Views)
		}
		plan.Views = append(plan.Views, ViewPlan{
			Identity: identity, Name: name, Type: name, Destination: planner.dests[view], Fields: fields, Ownership: ViewGenerated,
		})
	}
	for _, linked := range linkedViews {
		indexes[linked.identity] = len(plan.Views)
		plan.Views = append(plan.Views, linked.plan)
	}
	if err := planner.bindOutputFields(); err != nil {
		return nil, err
	}
	return indexes, nil
}

// Output-owned derived views still need generated row types and resources, but
// their holders belong to the response envelope rather than each parent row.
func (p *viewPlanner) bindOutputFields() error {
	for relation, param := range p.outputs {
		if strings.TrimSpace(param.TypeExpr) != "" || strings.TrimSpace(param.OutputTypeExpr) != "" || defaultTagTypeName(param.Tag) != "" {
			continue
		}
		cardinality, err := spec.NormalizeCardinality(relation.Cardinality)
		if err != nil {
			return err
		}
		typeName := "*" + p.names[relation.View]
		if cardinality == spec.CardinalityMany {
			typeName = "[]" + typeName
		}
		for i := range p.plan.Output.Fields {
			if p.plan.Output.Fields[i].Name == generatedParameterName(param) {
				p.plan.Output.Fields[i].Type = typeName
				break
			}
		}
	}
	return nil
}

func (r *planResolver) resolveLinkedView(view *spec.View, reference *ViewReference, identity string) (ViewPlan, error) {
	if reference == nil {
		return ViewPlan{}, fmt.Errorf("linked view %q reference is required", identity)
	}
	if r.types == nil {
		return ViewPlan{}, fmt.Errorf("linked view %q requires type authority", identity)
	}
	key := strings.TrimSpace(reference.DescriptorKey)
	if key == "" {
		return ViewPlan{}, fmt.Errorf("linked view %q descriptor key is required", identity)
	}
	descriptor, err := r.types.Descriptor(key)
	if err != nil {
		return ViewPlan{}, fmt.Errorf("resolve linked view %q: %w", key, err)
	}
	if descriptor == nil || strings.TrimSpace(descriptor.Name) == "" || strings.TrimSpace(descriptor.PkgPath) == "" {
		return ViewPlan{}, fmt.Errorf("linked view %q is not a named package type", key)
	}
	if explicit := strings.TrimSpace(view.TypeName); explicit != "" && explicit != descriptor.Name {
		return ViewPlan{}, fmt.Errorf("linked view type %q does not match package type %q", explicit, descriptor.Name)
	}
	if err := r.validateLinkedViewCasts(view, descriptor); err != nil {
		return ViewPlan{}, err
	}
	typeName := linkedNamedTypeExpression(r.plan, r.input.TargetPackage, descriptor)
	return ViewPlan{
		Identity: identity, Name: descriptor.Name, Type: typeName, Ownership: ViewLinked, DescriptorKey: key,
	}, nil
}

func (r *planResolver) independentViewType(view *spec.View, identity string) (string, error) {
	if explicit := strings.TrimSpace(view.TypeName); explicit != "" {
		return explicit, nil
	}
	name := view.CanonicalName()
	var expression string
	for _, param := range preferDefinedParams(r.input.Component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
			continue
		}
		paramViewName := strings.TrimSpace(param.Source.Name)
		if paramViewName == "" {
			paramViewName = strings.TrimSpace(param.Name)
		}
		if !strings.EqualFold(paramViewName, name) {
			continue
		}
		if boundIdentity := strings.TrimSpace(r.input.ViewBindings[param.Identity()]); boundIdentity != "" && boundIdentity != identity {
			continue
		}
		candidate := strings.TrimSpace(param.OutputTypeExpr)
		if candidate == "" {
			candidate = strings.TrimSpace(param.TypeExpr)
		}
		if candidate == "" {
			candidate = defaultTagTypeName(param.Tag)
		}
		if expression != "" && expression != candidate {
			return "", fmt.Errorf("independent view %q has conflicting parameter types %q and %q", name, expression, candidate)
		}
		expression = candidate
	}
	if base := unwrapQualifiedTypeName(expression); base != "" {
		if index := strings.LastIndex(base, "."); index != -1 {
			base = base[index+1:]
		}
		if token.IsIdentifier(base) && ast.IsExported(base) {
			return base, nil
		}
		return "", fmt.Errorf("independent view %q type %q must resolve to an exported Go identifier", name, expression)
	}
	typeName := typecatalog.FieldName(name)
	if !strings.HasSuffix(typeName, "View") {
		typeName += "View"
	}
	return typeName, nil
}

func (r *planResolver) bindIndependentViewFields(fields []Field) error {
	if r.plan == nil || r.input.Component == nil || len(fields) == 0 || len(r.viewIndexes) == 0 {
		return nil
	}
	for _, param := range preferDefinedParams(r.input.Component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
			continue
		}
		view, err := r.independentViewForParam(param)
		if err != nil {
			return err
		}
		if view == nil {
			return fmt.Errorf("parameter %q has no canonical independent view %q", param.Name, param.Source.Name)
		}
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		viewIndex, ok := r.viewIndexes[identity]
		if !ok || viewIndex < 0 || viewIndex >= len(r.plan.Views) {
			return fmt.Errorf("parameter %q has no planned independent view %q", param.Name, identity)
		}
		fieldName := exportedName(param.Name)
		found := false
		for index := range fields {
			if fields[index].Name != fieldName {
				continue
			}
			authoredType := strings.TrimSpace(param.OutputTypeExpr)
			if authoredType == "" {
				authoredType = strings.TrimSpace(param.TypeExpr)
			}
			if authoredType == "" {
				authoredType = defaultTagTypeName(param.Tag)
			}
			if authoredType == "" {
				switch strings.ToLower(strings.TrimSpace(param.Cardinality)) {
				case "one":
					fields[index].Type = "*" + r.plan.Views[viewIndex].Type
				case "", "many":
					fields[index].Type = "[]*" + r.plan.Views[viewIndex].Type
				default:
					return fmt.Errorf("parameter %q has unsupported independent view cardinality %q", param.Name, param.Cardinality)
				}
			} else {
				fields[index].Type = applyQualifiedWrapper(fields[index].Type, r.plan.Views[viewIndex].Type)
			}
			fields[index].Tag, err = appendViewTags(fields[index].Tag, view)
			if err != nil {
				return err
			}
			found = true
			break
		}
		if !found {
			return fmt.Errorf("parameter %q has no generated input field", param.Name)
		}
	}
	return nil
}

func (r *planResolver) independentViewForParam(param *spec.Parameter) (*spec.View, error) {
	views := r.input.Component.Views
	identity := r.input.ViewBindings[param.Identity()]
	if strings.TrimSpace(identity) != "" {
		for _, view := range views {
			if view == nil {
				continue
			}
			candidate, err := view.Identity()
			if err != nil {
				return nil, err
			}
			if candidate == identity {
				return view, nil
			}
		}
		return nil, fmt.Errorf("parameter %q targets unknown independent view %q", param.Name, identity)
	}
	name := strings.TrimSpace(param.Source.Name)
	if name == "" {
		name = strings.TrimSpace(param.Name)
	}
	var result *spec.View
	for _, view := range views {
		if view == nil || !strings.EqualFold(view.CanonicalName(), name) {
			continue
		}
		if result != nil {
			return nil, fmt.Errorf("parameter %q matches more than one independent view named %q", param.Name, name)
		}
		result = view
	}
	return result, nil
}

func (p *viewPlanner) assign(view *spec.View, typeName, destination string) error {
	if view == nil {
		return fmt.Errorf("generated view is required")
	}
	if p.visiting[view] {
		return fmt.Errorf("generated relation graph contains a cycle at view %q", view.Name)
	}
	if existing := p.names[view]; existing != "" {
		return nil
	}
	typeName = strings.TrimSpace(typeName)
	if typeName == "" {
		return fmt.Errorf("generated view %q has no type name", view.Name)
	}
	if !token.IsIdentifier(typeName) || !ast.IsExported(typeName) {
		return fmt.Errorf("generated view %q type %q must be an exported Go identifier", view.Name, typeName)
	}
	if owner := p.owners[typeName]; owner != nil && owner != view {
		return fmt.Errorf("generated views %q and %q map to type %q", owner.Name, view.Name, typeName)
	}
	p.names[view] = typeName
	p.owners[typeName] = view
	p.dests[view] = destination
	p.ordered = append(p.ordered, view)
	p.visiting[view] = true
	defer delete(p.visiting, view)
	for _, relation := range view.Relations {
		if relation == nil || relation.View == nil {
			return fmt.Errorf("generated view %q contains an incomplete relation", view.Name)
		}
		path := view.CanonicalName() + "." + strings.TrimSpace(relation.Holder)
		if previous, ok := p.parents[relation.View]; ok {
			return fmt.Errorf("generated relation tree shares view %q between %s and %s", relation.View.CanonicalName(), previous, path)
		}
		p.parents[relation.View] = path
		childDest, err := generatedViewDestination(relation.View, destination)
		if err != nil {
			return err
		}
		if err := p.assign(relation.View, generatedChildViewType(relation), childDest); err != nil {
			return err
		}
	}
	return nil
}

func generatedChildViewType(relation *spec.Relation) string {
	name := ""
	if relation != nil {
		if relation.View != nil {
			if explicit := strings.TrimSpace(relation.View.TypeName); explicit != "" {
				return explicit
			}
			name = relation.View.Name
		}
		if strings.TrimSpace(name) == "" {
			name = relation.Name
		}
		if strings.TrimSpace(name) == "" {
			name = relation.Holder
		}
	}
	name = typecatalog.FieldName(name)
	if !strings.HasSuffix(name, "View") {
		name += "View"
	}
	return name
}

func generatedViewDestination(view *spec.View, inherited string) (string, error) {
	destination := strings.TrimSpace(inherited)
	if view != nil && strings.TrimSpace(view.Dest) != "" {
		destination = strings.TrimSpace(view.Dest)
	}
	relative, err := managedRelativePath(destination)
	if err != nil {
		return "", fmt.Errorf("generated view %q destination: %w", view.CanonicalName(), err)
	}
	if relative == "" || !strings.EqualFold(filepath.Ext(relative), ".go") {
		return "", fmt.Errorf("generated view %q destination %q must be a Go source file", view.CanonicalName(), destination)
	}
	if filepath.Base(relative) != relative {
		return "", fmt.Errorf("generated view %q destination %q must be a file in the generated package", view.CanonicalName(), destination)
	}
	return relative, nil
}

func (p *viewPlanner) fields(view *spec.View) ([]Field, error) {
	fields := resolveScalarViewFields(p.plan, view, p.velty)
	seen := map[string]bool{}
	for _, field := range fields {
		if seen[field.Name] {
			return nil, fmt.Errorf("generated view %q contains duplicate field %q", view.Name, field.Name)
		}
		seen[field.Name] = true
	}
	for _, relation := range view.Relations {
		if _, output := p.outputs[relation]; output {
			continue
		}
		field, err := p.relationField(relation)
		if err != nil {
			return nil, err
		}
		if seen[field.Name] {
			return nil, fmt.Errorf("generated relation %q collides with scalar field %q", relation.Name, field.Name)
		}
		seen[field.Name] = true
		fields = append(fields, field)
	}
	if view.SelfReference != nil {
		holder := typecatalog.FieldName(view.SelfReference.Holder)
		if holder == "" {
			return nil, fmt.Errorf("generated self-reference on view %q requires a holder", view.Name)
		}
		if seen[holder] {
			return nil, fmt.Errorf("generated self-reference holder %q collides with another field", holder)
		}
		selfValue, err := (tag.SelfReference{Child: view.SelfReference.Child, Parent: view.SelfReference.Parent}).Value()
		if err != nil {
			return nil, fmt.Errorf("format generated self-reference on view %q: %w", view.Name, err)
		}
		selfTag := appendStructTag(`sqlx:"-"`, tag.SelfName, selfValue)
		fields = append(fields, Field{Name: holder, Type: "[]*" + p.names[view], Tag: selfTag})
	}
	return fields, nil
}

func resolveScalarViewFields(plan *Plan, view *spec.View, includeVelty bool) []Field {
	if plan == nil || view == nil {
		return nil
	}
	result := make([]Field, 0, len(view.Columns))
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		name := typecatalog.FieldName(column.Name)
		if name == "" {
			continue
		}
		effectiveType := column.EffectiveType()
		typeName := strings.TrimSpace(effectiveType.Name)
		if typeName == "" {
			typeName = "any"
		}
		if packagePath := strings.TrimSpace(effectiveType.Package); packagePath != "" {
			alias := uniqueImportAlias(plan, packagePath)
			ensureImport(plan, alias, packagePath)
			typeName = alias + "." + typeName
		}
		if effectiveType.Pointer && (column.ExplicitType || nullablePointerType(typeName)) {
			typeName = "*" + typeName
		}
		if effectiveType.Cardinality == spec.CardinalityMany {
			typeName = "[]" + typeName
		}
		if effectiveType.SlicePointer {
			typeName = "*" + typeName
		}
		source := strings.TrimSpace(column.Source)
		if source == "" {
			source = strings.TrimSpace(column.Name)
		}
		fieldTag := scalarColumnFieldTag(column, source, includeVelty)
		if column.Groupable != nil && *column.Groupable && !hasStructTag(fieldTag, "groupable") {
			fieldTag = appendStructTag(fieldTag, "groupable", "true")
		}
		result = append(result, Field{Name: name, Type: typeName, Tag: fieldTag, ExplicitType: column.ExplicitType})
	}
	return result
}

func (p *viewPlanner) relationField(relation *spec.Relation) (Field, error) {
	if relation == nil || relation.View == nil {
		return Field{}, fmt.Errorf("generated relation is incomplete")
	}
	name := typecatalog.FieldName(relation.Holder)
	if name == "" {
		name = typecatalog.FieldName(relation.Name)
	}
	if name == "" {
		return Field{}, fmt.Errorf("generated relation %q requires a holder", relation.Name)
	}
	typeName := p.names[relation.View]
	if typeName == "" {
		return Field{}, fmt.Errorf("generated relation %q has no child type", relation.Name)
	}
	fieldType := "[]*" + typeName
	if relation.Cardinality == spec.CardinalityOne {
		fieldType = "*" + typeName
	}
	links := make([]*tag.RelationLink, 0, len(relation.On))
	for _, link := range relation.On {
		if link == nil {
			continue
		}
		links = append(links, &tag.RelationLink{
			Parent: tag.RelationPart{Field: typecatalog.FieldName(link.ParentColumn), Namespace: link.ParentNamespace, Column: link.ParentColumn},
			Child:  tag.RelationPart{Field: typecatalog.FieldName(link.ChildColumn), Namespace: link.ChildNamespace, Column: link.ChildColumn},
		})
	}
	onValue, err := tag.RelationValue(links)
	if err != nil {
		return Field{}, fmt.Errorf("format generated relation %q on tag: %w", relation.Name, err)
	}
	fieldTag, err := appendRelationViewTags("", relation)
	if err != nil {
		return Field{}, fmt.Errorf("format generated relation %q view tag: %w", relation.Name, err)
	}
	if onValue != "" {
		fieldTag = appendStructTag(fieldTag, tag.RelationName, onValue)
	}
	if format := text.NewCaseFormat(p.plan.Settings.CaseFormat); format != text.CaseFormatUndefined {
		fieldTag = appendStructTag(fieldTag, "json", text.DetectCaseFormat(name).Format(name, format))
	}
	return Field{Name: name, Type: fieldType, Tag: fieldTag, RelationHolder: true}, nil
}

func appendViewTags(fieldTag string, view *spec.View) (string, error) {
	return appendViewTagsWithMatch(fieldTag, view, "")
}

func appendRelationViewTags(fieldTag string, relation *spec.Relation) (string, error) {
	if relation == nil {
		return fieldTag, nil
	}
	return appendViewTagsWithMatch(fieldTag, relation.View, string(relation.MatchStrategy))
}

func appendViewTagsWithMatch(fieldTag string, view *spec.View, match string) (string, error) {
	if view == nil {
		return fieldTag, nil
	}
	fieldTag = withoutStructTags(fieldTag, tag.ViewName, tag.SQLName)
	metadata := tag.View{Name: view.CanonicalName(), TypeName: view.TypeName, Dest: view.Dest, EntityHooks: view.EntityHooks, Batch: view.BatchSize,
		BatchConcurrency: view.BatchConcurrency,
		Auxiliary:        view.Auxiliary,
		Match:            match,
		PublishParent:    view.PublishParent, RelationalConcurrency: view.RelationalConcurrency,
		Partitioning: view.Partitioning.Clone(), Selector: view.Selector.Clone(),
		AllowNulls: cloneBoolValue(view.AllowNulls), Groupable: cloneBoolValue(view.Groupable)}
	if source := view.RuntimeSource(); source != nil {
		metadata.Table = source.Table
		metadata.URI = source.URI
		if source.Bindings != nil {
			metadata.Connector = source.Bindings.Connector
			metadata.Cache = source.Bindings.CacheName
			metadata.CacheWarmup = source.Bindings.CacheWarmup
		}
		if source.Controls != nil {
			metadata.OrderBy = source.Controls.OrderBy
			if source.Controls.Limit != nil {
				limit := *source.Controls.Limit
				metadata.Limit = &limit
			}
			if source.Controls.Offset != nil {
				offset := *source.Controls.Offset
				metadata.Offset = &offset
			}
		}
	}
	viewValue, err := metadata.Value()
	if err != nil {
		return "", err
	}
	if viewValue != "" {
		fieldTag = appendStructTag(fieldTag, tag.ViewName, viewValue)
	}
	if sqlValue := viewSQLValue(view); sqlValue != "" {
		fieldTag = appendStructTag(fieldTag, tag.SQLName, sqlValue)
	}
	return fieldTag, nil
}

func viewSQLValue(view *spec.View) string {
	source := view.RuntimeSource()
	if source == nil {
		return ""
	}
	if strings.TrimSpace(source.SQL) != "" {
		return (tag.SQL{Text: source.SQL}).Value()
	}
	if uri := strings.TrimSpace(source.URI); uri != "" {
		return (tag.SQL{URI: uri}).Value()
	}
	return ""
}

func cloneBoolValue(source *bool) *bool {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}

func nullablePointerType(typeName string) bool {
	typeName = strings.TrimSpace(typeName)
	return typeName != "" && typeName != "any" && typeName != "interface{}" &&
		!strings.HasPrefix(typeName, "*") && !strings.HasPrefix(typeName, "[]") &&
		!strings.HasPrefix(typeName, "map[")
}

func appendStructTag(fieldTag, name, value string) string {
	if fieldTag != "" {
		fieldTag += " "
	}
	return fieldTag + name + ":" + strconv.Quote(value)
}

func replaceStructTag(fieldTag, name, value string) string {
	filtered := withoutStructTags(fieldTag, name)
	return appendStructTag(filtered, name, value)
}
