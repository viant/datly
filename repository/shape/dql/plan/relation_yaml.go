package plan

import (
	"strings"

	"gopkg.in/yaml.v3"
)

func parseViewMetaNodes(rootNode *yaml.Node) map[string]*viewMeta {
	result := map[string]*viewMeta{}
	views := viewsNode(rootNode)
	if views == nil || views.Kind != yaml.SequenceNode {
		return result
	}
	for _, item := range views.Content {
		meta := parseViewMeta(item)
		if meta == nil || strings.TrimSpace(meta.Name) == "" {
			continue
		}
		result[meta.Name] = meta
	}
	return result
}

func parseViewMeta(item *yaml.Node) *viewMeta {
	viewMap := nodeMapping(item)
	if viewMap == nil {
		return nil
	}
	name := strings.TrimSpace(nodeString(mappingValue(viewMap, "Name")))
	if name == "" {
		return nil
	}
	meta := &viewMeta{
		Name:       name,
		Line:       item.Line,
		Aliases:    map[string]bool{},
		Namespaces: map[string]bool{},
		Projection: projectionMeta{Columns: map[string]bool{}},
	}
	parseViewTemplateMeta(viewMap, meta)
	parseViewSelectorMeta(viewMap, meta)
	parseViewRelationsMeta(viewMap, meta)
	return meta
}

func parseViewTemplateMeta(viewMap map[string]*yaml.Node, meta *viewMeta) {
	template := nodeMapping(mappingValue(viewMap, "Template"))
	sourceNode := mappingValue(template, "Source")
	if sourceNode == nil {
		return
	}
	aliases, projection, hasSQL := analyzeSQL(nodeString(sourceNode))
	meta.HasSQL = hasSQL
	if len(aliases) > 0 {
		meta.Aliases = aliases
	}
	if len(projection.Columns) > 0 || projection.HasStar {
		meta.Projection = projection
	}
}

func parseViewSelectorMeta(viewMap map[string]*yaml.Node, meta *viewMeta) {
	selector := nodeMapping(mappingValue(viewMap, "Selector"))
	registerAlias(meta.Namespaces, nodeString(mappingValue(selector, "Namespace")))
	for alias := range meta.Aliases {
		meta.Namespaces[alias] = true
	}
}

func parseViewRelationsMeta(viewMap map[string]*yaml.Node, meta *viewMeta) {
	with := mappingValue(viewMap, "With")
	if with == nil || with.Kind != yaml.SequenceNode {
		return
	}
	for _, relItem := range with.Content {
		rel := parseRelationMeta(relItem)
		if rel != nil {
			meta.Relations = append(meta.Relations, *rel)
		}
	}
}

func parseRelationMeta(relItem *yaml.Node) *relationMeta {
	relMap := nodeMapping(relItem)
	if relMap == nil {
		return nil
	}
	rel := &relationMeta{
		Line:   relItem.Line,
		Name:   nodeString(mappingValue(relMap, "Name")),
		Holder: nodeString(mappingValue(relMap, "Holder")),
		On:     parseLinkNodes(mappingValue(relMap, "On")),
	}
	ofMap := nodeMapping(mappingValue(relMap, "Of"))
	rel.Ref = nodeString(mappingValue(ofMap, "Ref"))
	if rel.Ref == "" {
		rel.Ref = nodeString(mappingValue(ofMap, "Name"))
	}
	rel.OfOn = parseLinkNodes(mappingValue(ofMap, "On"))
	rel.PairCount = len(rel.On)
	if len(rel.OfOn) > rel.PairCount {
		rel.PairCount = len(rel.OfOn)
	}
	return rel
}

func parseLinkNodes(seq *yaml.Node) []relationLink {
	if seq == nil || seq.Kind != yaml.SequenceNode {
		return nil
	}
	ret := make([]relationLink, 0, len(seq.Content))
	for _, item := range seq.Content {
		linkMap := nodeMapping(item)
		if linkMap == nil {
			ret = append(ret, relationLink{Line: item.Line})
			continue
		}
		ret = append(ret, relationLink{
			Line:      item.Line,
			Column:    nodeString(mappingValue(linkMap, "Column")),
			Namespace: nodeString(mappingValue(linkMap, "Namespace")),
		})
	}
	return ret
}

func viewsNode(rootNode *yaml.Node) *yaml.Node {
	rootMap := nodeMapping(rootNode)
	resource := mappingValue(rootMap, "Resource")
	resourceMap := nodeMapping(resource)
	return mappingValue(resourceMap, "Views")
}

func nodeMapping(n *yaml.Node) map[string]*yaml.Node {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	if n.Kind != yaml.MappingNode {
		return nil
	}
	ret := map[string]*yaml.Node{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		ret[n.Content[i].Value] = n.Content[i+1]
	}
	return ret
}

func mappingValue(m map[string]*yaml.Node, key string) *yaml.Node {
	if m == nil {
		return nil
	}
	return m[key]
}

func nodeString(n *yaml.Node) string {
	if n == nil {
		return ""
	}
	return strings.TrimSpace(n.Value)
}
