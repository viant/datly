package transcribe

import (
	"fmt"
	"strings"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// readGroupCompilation consumes the already refined semantic equality links.
// Independent reads with no such links get no inferred relationship groups.
type readGroupCompilation struct {
	reads      []plan.ReadCollection
	identities map[string][]plan.KeyPart
}

func (c *readGroupCompilation) compile(record *plan.RecordPlan) error {
	if record == nil {
		return nil
	}
	if record.Current != nil && len(record.Current.Keys) > 0 {
		path := strings.Join(record.Current.InputPath, ".")
		if c.identities == nil {
			c.identities = map[string][]plan.KeyPart{}
		}
		if prior, ok := c.identities[path]; ok {
			same := len(prior) == len(record.Current.Keys)
			if same {
				for i := range prior {
					same = same && prior[i].Field == record.Current.Keys[i].Field
				}
			}
			if !same {
				return fmt.Errorf("read %s has incompatible canonical identity keys", path)
			}
		} else {
			c.identities[path] = append([]plan.KeyPart(nil), record.Current.Keys...)
			for i := range c.reads {
				if strings.Join(c.reads[i].InputPath, ".") == path {
					c.reads[i].Keys = append([]plan.KeyPart(nil), record.Current.Keys...)
				}
			}
		}
	}
	for _, relation := range record.Relations {
		if relation == nil || relation.Child == nil {
			continue
		}
		var parent, child []plan.KeyPart
		for _, link := range relation.Links {
			parent = append(parent, link.Parent)
			child = append(child, link.Child)
		}
		if err := c.add(record, parent); err != nil {
			return err
		}
		if err := c.add(relation.Child, child); err != nil {
			return err
		}
		if err := c.compile(relation.Child); err != nil {
			return err
		}
	}
	for _, relation := range record.SelfRelations {
		var parent, child []plan.KeyPart
		for _, link := range relation.Links {
			parent = append(parent, link.Parent)
			child = append(child, link.Child)
		}
		if err := c.add(record, parent); err != nil {
			return err
		}
		if err := c.add(record, child); err != nil {
			return err
		}
	}
	return nil
}

func (c *readGroupCompilation) add(record *plan.RecordPlan, parts []plan.KeyPart) error {
	if record.Current == nil || len(parts) == 0 {
		return nil
	}
	for i := range c.reads {
		read := &c.reads[i]
		if strings.Join(read.InputPath, ".") != strings.Join(record.Current.InputPath, ".") {
			continue
		}
		group := plan.ReadGroup{}
		for _, part := range parts {
			var mapped *plan.FieldRef
			for _, field := range record.Current.Fields {
				if field.Entity.Field == part.Field {
					value := field.Current
					mapped = &value
					break
				}
			}
			var selected *plan.FieldRef
			for j := range read.Fields {
				field := &read.Fields[j]
				match := mapped != nil && mapped.Field == field.Field
				if mapped == nil {
					match = (part.Source != "" && part.Source == field.Source) || (part.Source == "" && part.Field == field.Field)
				}
				if !match {
					continue
				}
				if selected != nil {
					return fmt.Errorf("ambiguous read relationship field %s.%s", read.Name, part.Field)
				}
				selected = field
			}
			if selected == nil {
				return fmt.Errorf("read relationship field %s.%s has no canonical projection", read.Name, part.Field)
			}
			group.Parts = append(group.Parts, plan.KeyPart{Field: selected.Field, Source: selected.Source, Type: selected.Type})
		}
		for _, prior := range read.Groups {
			if len(prior.Parts) != len(group.Parts) {
				continue
			}
			same := true
			for n := range prior.Parts {
				same = same && prior.Parts[n].Field == group.Parts[n].Field
			}
			if same {
				return nil
			}
		}
		read.Groups = append(read.Groups, group)
		return nil
	}
	return fmt.Errorf("relationship Current path %v has no application read collection", record.Current.InputPath)
}
