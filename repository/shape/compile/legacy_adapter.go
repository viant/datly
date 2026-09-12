package compile

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
	"gopkg.in/yaml.v3"
)

func resolveGeneratedCompanionDQL(source *shape.Source) string {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return ""
	}
	settings := extractRuleSettings(source, nil)
	typeExpr := strings.TrimSpace(settings.Type)
	if typeExpr == "" {
		return ""
	}
	typeExpr = strings.TrimSuffix(typeExpr, ".Handler")
	typeExpr = strings.Trim(typeExpr, `"'`)
	if typeExpr == "" {
		return ""
	}
	dir := filepath.Dir(source.Path)
	baseTypePath := filepath.FromSlash(typeExpr)
	stem := filepath.Base(baseTypePath)
	candidates := []string{
		filepath.Join(dir, "gen", baseTypePath+".dql"),
		filepath.Join(dir, "gen", baseTypePath+".sql"),
		filepath.Join(dir, "gen", stem+".dql"),
		filepath.Join(dir, "gen", stem+".sql"),
	}
	for _, candidate := range candidates {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		content := strings.TrimSpace(string(data))
		if content != "" {
			return content
		}
	}
	return ""
}

func resolveLegacyRouteViews(source *shape.Source) []*plan.View {
	return resolveLegacyRouteViewsWithLayout(source, defaultCompilePathLayout())
}

func resolveLegacyRouteViewsWithLayout(source *shape.Source, layout compilePathLayout) []*plan.View {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return nil
	}
	platformRoot, relativeDir, stem, ok := platformPathParts(source.Path, layout)
	if !ok {
		return nil
	}
	settings := extractRuleSettings(source, nil)
	typeExpr := strings.TrimSpace(settings.Type)
	typeExpr = strings.Trim(typeExpr, `"'`)
	typeExpr = strings.TrimSuffix(typeExpr, ".Handler")
	typeStem := ""
	if typeExpr != "" {
		typeStem = filepath.Base(filepath.FromSlash(typeExpr))
	}
	routesRoot := joinRelativePath(platformRoot, layout.routesRelative)
	routesBase := filepath.Join(routesRoot, filepath.FromSlash(relativeDir))
	legacyMeta := []legacyViewMeta(nil)
	for _, candidateYAML := range legacyRouteYAMLCandidates(routesBase, stem, typeStem) {
		legacyMeta = loadLegacyRouteViewMeta(candidateYAML)
		if len(legacyMeta) > 0 {
			break
		}
	}
	searchDirs := []string{
		filepath.Join(routesBase, typeStem, stem),
		filepath.Join(routesBase, typeStem),
		filepath.Join(routesBase, stem, stem),
		filepath.Join(routesBase, stem),
		routesBase,
	}
	var sqlFiles []string
	for _, dir := range searchDirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".sql") {
				continue
			}
			sqlFiles = append(sqlFiles, filepath.Join(dir, entry.Name()))
		}
		if len(sqlFiles) > 0 {
			break
		}
	}
	if len(sqlFiles) == 0 {
		return nil
	}
	sort.Strings(sqlFiles)
	result := make([]*plan.View, 0, len(sqlFiles))
	rootIndex := -1
	for _, sqlFile := range sqlFiles {
		name := strings.TrimSuffix(filepath.Base(sqlFile), filepath.Ext(sqlFile))
		if name == "" {
			continue
		}
		data, err := os.ReadFile(sqlFile)
		if err != nil {
			continue
		}
		sqlText := string(data)
		table := ""
		if name != stem {
			table = inferTableFromSQL(sqlText, source)
		}
		connector := strings.TrimSpace(settings.Connector)
		if connector == "" {
			connector = strings.TrimSpace(source.Connector)
		}
		if connector == "" {
			connector = inferConnector(&plan.View{Table: table}, source)
		}
		viewItem := &plan.View{
			Path:        name,
			Holder:      name,
			Name:        name,
			Table:       table,
			SQL:         sqlText,
			SQLURI:      filepath.ToSlash(filepath.Join(stem, name+".sql")),
			Connector:   connector,
			Cardinality: "many",
			FieldType:   reflect.TypeOf([]map[string]interface{}{}),
			ElementType: reflect.TypeOf(map[string]interface{}{}),
		}
		if meta, ok := lookupLegacyViewMeta(legacyMeta, name); ok {
			if strings.TrimSpace(meta.Table) != "" {
				viewItem.Table = strings.TrimSpace(meta.Table)
			}
			if strings.TrimSpace(meta.Connector) != "" {
				viewItem.Connector = strings.TrimSpace(meta.Connector)
			}
			if strings.TrimSpace(meta.SQLURI) != "" {
				viewItem.SQLURI = strings.TrimSpace(meta.SQLURI)
			}
		}
		if name == stem {
			rootIndex = len(result)
		}
		result = append(result, viewItem)
	}
	if len(result) == 0 {
		return nil
	}
	if rootIndex > 0 {
		root := result[rootIndex]
		copy(result[1:rootIndex+1], result[0:rootIndex])
		result[0] = root
	}
	if result[0].Name != stem {
		rootConnector := result[0].Connector
		result = append([]*plan.View{{
			Path:        stem,
			Holder:      stem,
			Name:        stem,
			Table:       "",
			SQLURI:      filepath.ToSlash(filepath.Join(stem, stem+".sql")),
			Connector:   rootConnector,
			Cardinality: "many",
			FieldType:   reflect.TypeOf([]map[string]interface{}{}),
			ElementType: reflect.TypeOf(map[string]interface{}{}),
		}}, result...)
	}
	result[0].Table = ""
	result[0].Name = stem
	result[0].Holder = stem
	result[0].Path = stem
	if meta, ok := lookupLegacyViewMeta(legacyMeta, stem); ok {
		if strings.TrimSpace(meta.Table) != "" {
			result[0].Table = strings.TrimSpace(meta.Table)
		}
		if strings.TrimSpace(meta.Connector) != "" {
			result[0].Connector = strings.TrimSpace(meta.Connector)
		}
		if strings.TrimSpace(meta.SQLURI) != "" {
			result[0].SQLURI = strings.TrimSpace(meta.SQLURI)
		}
	}
	if result[0].SQLURI == "" {
		result[0].SQLURI = filepath.ToSlash(filepath.Join(stem, stem+".sql"))
	}
	return result
}

type legacyViewMeta struct {
	Name      string
	Table     string
	Connector string
	SQLURI    string
}

func loadLegacyRouteViewMeta(yamlPath string) []legacyViewMeta {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil
	}
	var payload struct {
		Resource struct {
			Views []struct {
				Name      string `yaml:"Name"`
				Table     string `yaml:"Table"`
				Connector struct {
					Ref string `yaml:"Ref"`
				} `yaml:"Connector"`
				Template struct {
					SourceURL string `yaml:"SourceURL"`
				} `yaml:"Template"`
			} `yaml:"Views"`
		} `yaml:"Resource"`
	}
	if err = yaml.Unmarshal(data, &payload); err != nil {
		return nil
	}
	result := make([]legacyViewMeta, 0, len(payload.Resource.Views))
	for _, item := range payload.Resource.Views {
		result = append(result, legacyViewMeta{
			Name:      strings.TrimSpace(item.Name),
			Table:     strings.TrimSpace(item.Table),
			Connector: strings.TrimSpace(item.Connector.Ref),
			SQLURI:    strings.TrimSpace(item.Template.SourceURL),
		})
	}
	return result
}

func lookupLegacyViewMeta(items []legacyViewMeta, name string) (legacyViewMeta, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return legacyViewMeta{}, false
	}
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.Name), name) {
			return item, true
		}
	}
	return legacyViewMeta{}, false
}

func legacyRouteYAMLCandidates(routesBase, stem, typeStem string) []string {
	stemFileVariants := routeStemAlternatives(stem)
	stemDirVariants := routeStemAlternatives(stem)
	typeVariants := routeStemAlternatives(typeStem)
	var result []string
	seen := map[string]bool{}
	appendCandidate := func(path string) {
		path = filepath.Clean(path)
		if path == "." || path == "" || seen[path] {
			return
		}
		seen[path] = true
		result = append(result, path)
	}
	for _, fileStem := range stemFileVariants {
		appendCandidate(filepath.Join(routesBase, fileStem+".yaml"))
		for _, dirStem := range stemDirVariants {
			appendCandidate(filepath.Join(routesBase, dirStem, fileStem+".yaml"))
		}
		for _, itemTypeStem := range typeVariants {
			if strings.TrimSpace(itemTypeStem) == "" {
				continue
			}
			appendCandidate(filepath.Join(routesBase, itemTypeStem, fileStem+".yaml"))
		}
	}
	return result
}

func routeStemAlternatives(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	alts := []string{value}
	dashed := strings.ReplaceAll(value, "_", "-")
	if dashed != value {
		alts = append(alts, dashed)
	}
	return alts
}

func platformPathParts(sourcePath string, layout compilePathLayout) (platformRoot, relativeDir, stem string, ok bool) {
	sourcePath = filepath.Clean(strings.TrimSpace(sourcePath))
	if sourcePath == "" {
		return "", "", "", false
	}
	normalized := filepath.ToSlash(sourcePath)
	marker := layout.dqlMarker
	if marker == "" {
		marker = defaultCompilePathLayout().dqlMarker
	}
	idx := strings.Index(normalized, marker)
	if idx == -1 {
		return "", "", "", false
	}
	platformRoot = sourcePath[:idx]
	relative := strings.TrimPrefix(normalized[idx+len(marker):], "/")
	relativeDir = filepath.Dir(relative)
	stem = strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
	if strings.TrimSpace(stem) == "" {
		return "", "", "", false
	}
	return platformRoot, relativeDir, stem, true
}
