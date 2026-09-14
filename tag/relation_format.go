package tag

import (
	"fmt"
	"strings"
)

// RelationValue formats typed relation links using ParseRelation's grammar.
func RelationValue(links []*RelationLink) (string, error) {
	values := make([]string, 0, len(links))
	for _, link := range links {
		if link == nil {
			continue
		}
		parent, err := relationPartValue(link.Parent, true)
		if err != nil {
			return "", err
		}
		child, err := relationPartValue(link.Child, false)
		if err != nil {
			return "", err
		}
		values = append(values, parent+"="+child)
	}
	return strings.Join(values, ","), nil
}

func relationPartValue(part RelationPart, allowInclude bool) (string, error) {
	column := strings.TrimSpace(part.Column)
	if column == "" {
		return "", fmt.Errorf("relation column is required")
	}
	if namespace := strings.TrimSpace(part.Namespace); namespace != "" {
		column = namespace + "." + column
	}
	value := column
	if field := strings.TrimSpace(part.Field); field != "" {
		value = field + ":" + value
	}
	if part.Include != nil {
		if !allowInclude {
			return "", fmt.Errorf("relation include flag is only valid on the parent side")
		}
		value += fmt.Sprintf("(%t)", *part.Include)
	}
	if _, err := parseRelationPart(value); err != nil {
		return "", err
	}
	return value, nil
}

// Value formats SQL metadata using ParseSQL's grammar.
func (s SQL) Value() string {
	if uri := strings.TrimSpace(s.URI); uri != "" {
		return "uri=" + uri
	}
	return strings.TrimSpace(s.Text)
}
