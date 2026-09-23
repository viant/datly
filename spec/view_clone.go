package spec

func (v *View) Clone() *View {
	return cloneView(v, map[*View]*View{})
}

func cloneView(source *View, cloned map[*View]*View) *View {
	if source == nil {
		return nil
	}
	if existing := cloned[source]; existing != nil {
		return existing
	}
	result := &View{
		Key: source.Key, Name: source.Name, Namespace: source.Namespace, TypeName: source.TypeName, Dest: source.Dest, EntityHooks: source.EntityHooks,
		Cardinality: source.Cardinality,
		InMemory:    source.InMemory,
		AllowNulls:  cloneBool(source.AllowNulls), Groupable: cloneBool(source.Groupable), Auxiliary: source.Auxiliary,
		Columns: cloneColumns(source.Columns),
		Source:  source.Source.Clone(), Selector: source.Selector.Clone(),
		Partitioning: source.Partitioning.Clone(), SelfReference: cloneSelfReference(source.SelfReference),
		BatchSize: source.BatchSize, BatchConcurrency: source.BatchConcurrency, PublishParent: source.PublishParent, RelationalConcurrency: source.RelationalConcurrency,
	}
	cloned[source] = result
	for _, relation := range source.Relations {
		if relation == nil {
			result.Relations = append(result.Relations, nil)
			continue
		}
		item := *relation
		item.View = cloneView(relation.View, cloned)
		item.On = make([]*RelationLink, 0, len(relation.On))
		for _, link := range relation.On {
			if link == nil {
				item.On = append(item.On, nil)
				continue
			}
			copied := *link
			item.On = append(item.On, &copied)
		}
		result.Relations = append(result.Relations, &item)
	}
	return result
}

func cloneBool(source *bool) *bool {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}

func (s *Selector) Clone() *Selector {
	if s == nil {
		return nil
	}
	result := *s
	result.Filterable = append([]FieldPath(nil), s.Filterable...)
	result.SQLMethods = make([]SQLMethod, len(s.SQLMethods))
	for i, method := range s.SQLMethods {
		result.SQLMethods[i] = SQLMethod{Name: method.Name, Args: append([]string(nil), method.Args...)}
	}
	result.Orderable = append([]FieldPath(nil), s.Orderable...)
	if s.OrderAliases != nil {
		result.OrderAliases = make(map[string]FieldPath, len(s.OrderAliases))
		for key, value := range s.OrderAliases {
			result.OrderAliases[key] = value
		}
	}
	return &result
}

func (p *Partitioning) Clone() *Partitioning {
	if p == nil {
		return nil
	}
	result := *p
	result.Arguments = append([]string(nil), p.Arguments...)
	return &result
}

func cloneSelfReference(source *SelfReference) *SelfReference {
	if source == nil {
		return nil
	}
	result := *source
	return &result
}
