package ast

// Clone returns a detached semantic tree suitable for build-stage refinement.
func (p *Plan) Clone() *Plan {
	if p == nil {
		return nil
	}
	result := *p
	result.Input = cloneContractRef(p.Input)
	if p.Output != nil {
		output := cloneContractRef(*p.Output)
		result.Output = &output
	}
	result.Root = cloneRecord(p.Root, map[*RecordPlan]*RecordPlan{})
	return &result
}

func cloneContractRef(source ContractRef) ContractRef {
	result := source
	result.Path = cloneFieldPath(source.Path)
	return result
}

func cloneRecord(source *RecordPlan, records map[*RecordPlan]*RecordPlan) *RecordPlan {
	if source == nil {
		return nil
	}
	if cloned := records[source]; cloned != nil {
		return cloned
	}
	result := *source
	records[source] = &result
	result.InputPath = cloneFieldPath(source.InputPath)
	result.Keys = append([]KeyPart(nil), source.Keys...)
	result.PresenceFields = append([]string(nil), source.PresenceFields...)
	result.Entity = source.Entity.Clone()
	result.Current = cloneCurrent(source.Current)
	result.Sequence = cloneSequence(source.Sequence)
	result.Write = cloneWritePolicy(source.Write)
	result.Relations = make([]*RelationPlan, len(source.Relations))
	for index, relation := range source.Relations {
		result.Relations[index] = cloneRelation(relation, records)
	}
	result.SelfRelations = append([]SelfRelationPlan(nil), source.SelfRelations...)
	for index := range result.SelfRelations {
		result.SelfRelations[index].FieldPath = cloneFieldPath(source.SelfRelations[index].FieldPath)
		result.SelfRelations[index].Links = append([]KeyLink(nil), source.SelfRelations[index].Links...)
	}
	return &result
}

func cloneCurrent(source *CurrentPlan) *CurrentPlan {
	if source == nil {
		return nil
	}
	result := *source
	result.InputPath = cloneFieldPath(source.InputPath)
	result.Keys = append([]KeyPart(nil), source.Keys...)
	result.Fields = append([]CurrentField(nil), source.Fields...)
	result.Self = append([]FieldRef(nil), source.Self...)
	return &result
}

func cloneSequence(source *SequencePlan) *SequencePlan {
	if source == nil {
		return nil
	}
	result := *source
	result.Destination = cloneFieldPath(source.Destination)
	result.Selector = cloneFieldPath(source.Selector)
	return &result
}

func cloneWritePolicy(source WritePolicy) WritePolicy {
	result := source
	result.ValuePath = cloneFieldPath(source.ValuePath)
	result.Allowed = append([]Action(nil), source.Allowed...)
	return result
}

func cloneRelation(source *RelationPlan, records map[*RecordPlan]*RecordPlan) *RelationPlan {
	if source == nil {
		return nil
	}
	result := *source
	result.FieldPath = cloneFieldPath(source.FieldPath)
	result.Links = append([]KeyLink(nil), source.Links...)
	result.Child = cloneRecord(source.Child, records)
	return &result
}

func cloneFieldPath(source FieldPath) FieldPath {
	return append(FieldPath(nil), source...)
}
