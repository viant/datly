package resource

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/viant/mcp-protocol/schema"
)

// DiscoveryOptions selects compact skill summaries or exact resource descriptors.
// It is an additive tool contract; native SEP inventories remain unchanged.
type DiscoveryOptions struct {
	Kind     string  `json:"kind,omitempty"`
	SkillURI string  `json:"skillURI,omitempty"`
	Query    string  `json:"query,omitempty"`
	Prefix   string  `json:"prefix,omitempty"`
	Group    string  `json:"group,omitempty"`
	Limit    int     `json:"limit,omitempty"`
	Cursor   *string `json:"cursor,omitempty"`
}

// UnmarshalJSON rejects misspelled filters and null/scalar coercions rather than
// silently broadening an agent's targeted discovery request.
func (o *DiscoveryOptions) UnmarshalJSON(raw []byte) error {
	type plain DiscoveryOptions
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("%s must not be null", key)
		}
	}
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*o = DiscoveryOptions(decoded)
	return nil
}

type SkillSummary struct {
	URI           string `json:"uri"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	Group         string `json:"group"`
	ResourceCount int    `json:"resourceCount"`
	Dynamic       bool   `json:"dynamic"`
}

type SkillGroup struct {
	Name          string `json:"name"`
	SkillCount    int    `json:"skillCount"`
	ResourceCount int    `json:"resourceCount"`
}

type DiscoveryResult struct {
	ResultType string                 `json:"resultType"`
	Kind       string                 `json:"kind"`
	Skills     []SkillSummary         `json:"skills"`
	Resources  []schema.SkillResource `json:"resources"`
	Groups     []SkillGroup           `json:"groups"`
	Total      int                    `json:"total"`
	NextCursor *string                `json:"nextCursor,omitempty"`
	TTLMillis  int                    `json:"ttlMs"`
	CacheScope string                 `json:"cacheScope"`
}

// Discover receives only caller-visible skills. Cursors bind the full visible
// inventory and normalized options, including mode, filters and page size.
// Resources mode searches resource URIs, never file contents; it does not fetch.
func Discover(entries []schema.Skill, options DiscoveryOptions) (*DiscoveryResult, error) {
	options.Query = strings.TrimSpace(strings.ToLower(options.Query))
	options.Group = strings.TrimSpace(options.Group)
	if options.Kind == "" {
		options.Kind = "skills"
	}
	if options.Kind != "skills" && options.Kind != "resources" {
		return nil, fmt.Errorf("kind must be skills or resources")
	}
	if options.Kind == "skills" && (options.SkillURI != "" || options.Prefix != "") {
		return nil, fmt.Errorf("skillURI and prefix require resources kind")
	}
	if options.Limit == 0 {
		options.Limit = 32
	}
	if options.Limit < 1 || options.Limit > 100 {
		return nil, fmt.Errorf("limit must be between 1 and 100")
	}
	if len(options.Query) > 512 || len(options.Group) > 256 || len(options.Prefix) > 4096 || len(options.SkillURI) > 4096 {
		return nil, fmt.Errorf("discovery filter is too long")
	}
	ordered := append([]schema.Skill(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Uri < ordered[j].Uri })
	result := &DiscoveryResult{ResultType: schema.ResultTypeComplete, Kind: options.Kind, Skills: []SkillSummary{}, Resources: []schema.SkillResource{}, Groups: []SkillGroup{}, TTLMillis: 0, CacheScope: "private"}
	groups := map[string]*SkillGroup{}
	summaries := []SkillSummary{}
	resources := []schema.SkillResource{}
	seen := map[string]bool{}
	for _, entry := range ordered {
		name, _ := entry.Frontmatter["name"].(string)
		description, _ := entry.Frontmatter["description"].(string)
		group, _ := entry.Frontmatter["group"].(string)
		if group == "" {
			group = name
		}
		if group == "" {
			group = entry.Uri
		}
		if options.Group != "" && options.Group != group {
			continue
		}
		count := len(entry.Resources.Files)
		if options.Kind == "resources" {
			if options.SkillURI != "" && entry.Uri != options.SkillURI {
				continue
			}
			count = 0
			for _, file := range entry.Resources.Files {
				if options.Prefix != "" && !strings.HasPrefix(file.Uri, options.Prefix) {
					continue
				}
				if options.Query != "" && !strings.Contains(strings.ToLower(file.Uri), options.Query) {
					continue
				}
				count++
				if !seen[file.Uri] {
					resources = append(resources, file)
					seen[file.Uri] = true
				}
			}
			if count == 0 {
				continue
			}
		} else {
			if options.Query != "" && !strings.Contains(strings.ToLower(name+"\n"+description+"\n"+group+"\n"+entry.Uri), options.Query) {
				continue
			}
			summaries = append(summaries, SkillSummary{URI: entry.Uri, Name: name, Description: description, Group: group, ResourceCount: count, Dynamic: entry.Resources.Dynamic})
		}
		if groups[group] == nil {
			groups[group] = &SkillGroup{Name: group}
		}
		groups[group].SkillCount++
		groups[group].ResourceCount += count
	}
	for _, group := range groups {
		result.Groups = append(result.Groups, *group)
	}
	sort.Slice(result.Groups, func(i, j int) bool { return result.Groups[i].Name < result.Groups[j].Name })
	sort.Slice(resources, func(i, j int) bool { return resources[i].Uri < resources[j].Uri })
	result.Total = len(summaries)
	if options.Kind == "resources" {
		result.Total = len(resources)
	}
	filters := options
	filters.Cursor = nil
	raw, err := json.Marshal(struct {
		Entries []schema.Skill
		Options DiscoveryOptions
	}{ordered, filters})
	if err != nil {
		return nil, fmt.Errorf("skill catalog encoding failed")
	}
	revision := fmt.Sprintf("%x", sha256.Sum256(raw))
	start := 0
	if options.Cursor != nil {
		decoded, err := base64.RawURLEncoding.DecodeString(*options.Cursor)
		parts := strings.Split(string(decoded), ":")
		if err != nil || len(parts) != 2 || parts[0] != revision {
			return nil, fmt.Errorf("invalid or stale discovery cursor")
		}
		start, err = strconv.Atoi(parts[1])
		if err != nil || start <= 0 || start >= result.Total || start%options.Limit != 0 {
			return nil, fmt.Errorf("invalid discovery cursor")
		}
	}
	end := min(start+options.Limit, result.Total)
	if options.Kind == "resources" {
		result.Resources = resources[start:end]
	} else {
		result.Skills = summaries[start:end]
	}
	if end < result.Total {
		next := base64.RawURLEncoding.EncodeToString([]byte(revision + ":" + strconv.Itoa(end)))
		result.NextCursor = &next
	}
	return result, nil
}

// SkillDiscoveryInputSchema advertises the same additive contract to all hosts.
func SkillDiscoveryInputSchema() schema.ToolInputSchema {
	return schema.ToolInputSchema{Type: "object", AdditionalProperties: map[string]interface{}{"additionalProperties": false}, Properties: schema.ToolInputSchemaProperties{
		"kind":     {"type": "string", "enum": []string{"skills", "resources"}, "default": "skills", "description": "Compact skill summaries or targeted static resource descriptors."},
		"skillURI": {"type": "string", "maxLength": 4096, "description": "Exact listed skill entrypoint URI; resources mode only."},
		"query":    {"type": "string", "maxLength": 512, "description": "Case insensitive substring; matches resource URIs in resources mode."},
		"prefix":   {"type": "string", "maxLength": 4096, "description": "Exact resource URI prefix; resources mode only."},
		"group":    {"type": "string", "maxLength": 256, "description": "Exact group returned by discovery."},
		"limit":    {"type": "integer", "minimum": 1, "maximum": 100, "default": 32},
		"cursor":   {"type": "string", "description": "Next cursor; retain mode, filters and limit."},
	}}
}
