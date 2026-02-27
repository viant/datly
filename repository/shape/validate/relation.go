package validate

import (
	"fmt"
	"strings"

	"github.com/viant/datly/view"
)

// ValidateRelations validates that relation link columns can be resolved on both
// parent and referenced views. It accepts alias/source/field variants and
// namespace-qualified forms (e.g. t.ID -> ID).
func ValidateRelations(resource *view.Resource, targets ...*view.View) error {
	if resource == nil {
		return nil
	}
	views := targets
	if len(views) == 0 {
		views = resource.Views
	}
	index := resource.Views.Index()
	var issues []string
	for _, parent := range views {
		if parent == nil {
			continue
		}
		parentIndex := view.Columns(parent.Columns).Index(parent.CaseFormat)
		for _, rel := range parent.With {
			if rel == nil || rel.Of == nil {
				continue
			}
			ref := &rel.Of.View
			if ref.Ref != "" {
				if lookup, err := index.Lookup(ref.Ref); err == nil && lookup != nil {
					ref = lookup
				}
			}
			refIndex := view.Columns(ref.Columns).Index(ref.CaseFormat)
			pairCount := len(rel.On)
			if len(rel.Of.On) > pairCount {
				pairCount = len(rel.Of.On)
			}
			for i := 0; i < pairCount; i++ {
				var parentLink, refLink *view.Link
				if i < len(rel.On) {
					parentLink = rel.On[i]
				}
				if i < len(rel.Of.On) {
					refLink = rel.Of.On[i]
				}

				if missing := missingColumn(parentIndex, parentLink); missing != "" {
					issues = append(issues, fmt.Sprintf("relation %q (parent=%q holder=%q link=%d): missing parent column %q", relName(rel, i), parent.Name, rel.Holder, i, missing))
				}
				if missing := missingColumn(refIndex, refLink); missing != "" {
					issues = append(issues, fmt.Sprintf("relation %q (parent=%q ref=%q holder=%q link=%d): missing ref column %q", relName(rel, i), parent.Name, ref.Name, rel.Holder, i, missing))
				}
			}
		}
	}
	if len(issues) == 0 {
		return nil
	}
	return fmt.Errorf("shape relation validation failed:\n- %s", strings.Join(issues, "\n- "))
}

func missingColumn(index view.NamedColumns, link *view.Link) string {
	if link == nil {
		return "<link undefined>"
	}
	for _, candidate := range linkCandidates(link) {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		if _, err := index.Lookup(candidate); err == nil {
			return ""
		}
	}
	for _, candidate := range linkCandidates(link) {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return "<empty>"
}

func linkCandidates(link *view.Link) []string {
	if link == nil {
		return nil
	}
	var result []string
	add := func(v string) {
		v = strings.TrimSpace(trimIdentifier(v))
		if v == "" {
			return
		}
		result = append(result, v)
		if i := strings.LastIndex(v, "."); i != -1 && i < len(v)-1 {
			result = append(result, v[i+1:])
		}
	}
	add(link.Column)
	if link.Namespace != "" && link.Column != "" {
		add(link.Namespace + "." + link.Column)
	}
	add(link.Field)
	return dedupe(result)
}

func trimIdentifier(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "`")
	value = strings.Trim(value, "\"")
	value = strings.Trim(value, "'")
	return value
}

func dedupe(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, value := range values {
		key := strings.ToLower(strings.TrimSpace(value))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, value)
	}
	return result
}

func relName(rel *view.Relation, idx int) string {
	if rel == nil {
		return fmt.Sprintf("#%d", idx)
	}
	if strings.TrimSpace(rel.Name) != "" {
		return rel.Name
	}
	return fmt.Sprintf("#%d", idx)
}
