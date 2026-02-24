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

func resolveLegacyRouteStates(source *shape.Source) []*plan.State {
	return resolveLegacyRouteStatesWithLayout(source, defaultCompilePathLayout())
}

func resolveLegacyRouteStatesWithLayout(source *shape.Source, layout compilePathLayout) []*plan.State {
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
	yamlCandidates := legacyRouteYAMLCandidates(routesBase, stem, typeStem)
	var payload struct {
		Resource struct {
			Parameters []struct {
				Name      string `yaml:"Name"`
				URI       string `yaml:"URI"`
				Value     string `yaml:"Value"`
				Required  *bool  `yaml:"Required"`
				Cacheable *bool  `yaml:"Cacheable"`
				In        struct {
					Kind string `yaml:"Kind"`
					Name string `yaml:"Name"`
				} `yaml:"In"`
				Predicates []struct {
					Group  int      `yaml:"Group"`
					Name   string   `yaml:"Name"`
					Ensure bool     `yaml:"Ensure"`
					Args   []string `yaml:"Args"`
				} `yaml:"Predicates"`
			} `yaml:"Parameters"`
			Views []struct {
				Name     string `yaml:"Name"`
				Selector struct {
					LimitParameter struct {
						Name      string `yaml:"Name"`
						Cacheable *bool  `yaml:"Cacheable"`
						In        struct {
							Kind string `yaml:"Kind"`
							Name string `yaml:"Name"`
						} `yaml:"In"`
					} `yaml:"LimitParameter"`
					OffsetParameter struct {
						Name      string `yaml:"Name"`
						Cacheable *bool  `yaml:"Cacheable"`
						In        struct {
							Kind string `yaml:"Kind"`
							Name string `yaml:"Name"`
						} `yaml:"In"`
					} `yaml:"OffsetParameter"`
					PageParameter struct {
						Name      string `yaml:"Name"`
						Cacheable *bool  `yaml:"Cacheable"`
						In        struct {
							Kind string `yaml:"Kind"`
							Name string `yaml:"Name"`
						} `yaml:"In"`
					} `yaml:"PageParameter"`
					FieldsParameter struct {
						Name      string `yaml:"Name"`
						Cacheable *bool  `yaml:"Cacheable"`
						In        struct {
							Kind string `yaml:"Kind"`
							Name string `yaml:"Name"`
						} `yaml:"In"`
					} `yaml:"FieldsParameter"`
					OrderByParameter struct {
						Name      string `yaml:"Name"`
						Cacheable *bool  `yaml:"Cacheable"`
						In        struct {
							Kind string `yaml:"Kind"`
							Name string `yaml:"Name"`
						} `yaml:"In"`
					} `yaml:"OrderByParameter"`
				} `yaml:"Selector"`
			} `yaml:"Views"`
		} `yaml:"Resource"`
	}
	loaded := false
	for _, candidate := range yamlCandidates {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		if err = yaml.Unmarshal(data, &payload); err != nil {
			continue
		}
		loaded = true
		break
	}
	if !loaded || len(payload.Resource.Parameters) == 0 {
		return nil
	}
	result := make([]*plan.State, 0, len(payload.Resource.Parameters))
	for _, item := range payload.Resource.Parameters {
		stateItem := &plan.State{
			Name:      strings.TrimSpace(item.Name),
			Path:      strings.TrimSpace(item.Name),
			Kind:      strings.TrimSpace(item.In.Kind),
			In:        strings.TrimSpace(item.In.Name),
			URI:       strings.TrimSpace(item.URI),
			Value:     strings.TrimSpace(item.Value),
			Required:  item.Required,
			Cacheable: item.Cacheable,
		}
		for _, predicate := range item.Predicates {
			stateItem.Predicates = append(stateItem.Predicates, &plan.StatePredicate{
				Group:     predicate.Group,
				Name:      strings.TrimSpace(predicate.Name),
				Ensure:    predicate.Ensure,
				Arguments: append([]string{}, predicate.Args...),
			})
		}
		result = append(result, stateItem)
	}
	seen := map[string]bool{}
	for _, item := range result {
		if item == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(item.Name)) + "|" + strings.ToLower(strings.TrimSpace(item.Kind)) + "|" + strings.ToLower(strings.TrimSpace(item.In))
		seen[key] = true
	}
	for _, viewItem := range payload.Resource.Views {
		selectorName := strings.TrimSpace(viewItem.Name)
		for _, param := range []struct {
			Name      string
			Cacheable *bool
			InKind    string
			InName    string
		}{
			{
				Name:      strings.TrimSpace(viewItem.Selector.LimitParameter.Name),
				Cacheable: viewItem.Selector.LimitParameter.Cacheable,
				InKind:    strings.TrimSpace(viewItem.Selector.LimitParameter.In.Kind),
				InName:    strings.TrimSpace(viewItem.Selector.LimitParameter.In.Name),
			},
			{
				Name:      strings.TrimSpace(viewItem.Selector.OffsetParameter.Name),
				Cacheable: viewItem.Selector.OffsetParameter.Cacheable,
				InKind:    strings.TrimSpace(viewItem.Selector.OffsetParameter.In.Kind),
				InName:    strings.TrimSpace(viewItem.Selector.OffsetParameter.In.Name),
			},
			{
				Name:      strings.TrimSpace(viewItem.Selector.PageParameter.Name),
				Cacheable: viewItem.Selector.PageParameter.Cacheable,
				InKind:    strings.TrimSpace(viewItem.Selector.PageParameter.In.Kind),
				InName:    strings.TrimSpace(viewItem.Selector.PageParameter.In.Name),
			},
			{
				Name:      strings.TrimSpace(viewItem.Selector.FieldsParameter.Name),
				Cacheable: viewItem.Selector.FieldsParameter.Cacheable,
				InKind:    strings.TrimSpace(viewItem.Selector.FieldsParameter.In.Kind),
				InName:    strings.TrimSpace(viewItem.Selector.FieldsParameter.In.Name),
			},
			{
				Name:      strings.TrimSpace(viewItem.Selector.OrderByParameter.Name),
				Cacheable: viewItem.Selector.OrderByParameter.Cacheable,
				InKind:    strings.TrimSpace(viewItem.Selector.OrderByParameter.In.Kind),
				InName:    strings.TrimSpace(viewItem.Selector.OrderByParameter.In.Name),
			},
		} {
			if param.Name == "" {
				continue
			}
			kind := firstNonEmptyString(strings.ToLower(param.InKind), "query")
			inName := firstNonEmptyString(param.InName, strings.ToLower(param.Name))
			key := strings.ToLower(param.Name) + "|" + kind + "|" + strings.ToLower(inName)
			if seen[key] {
				continue
			}
			item := &plan.State{
				Name:          param.Name,
				Path:          param.Name,
				Kind:          kind,
				In:            inName,
				QuerySelector: selectorName,
				Cacheable:     param.Cacheable,
			}
			result = append(result, item)
			seen[key] = true
		}
	}
	return result
}

func resolveLegacyRouteTypes(source *shape.Source) []*plan.Type {
	return resolveLegacyRouteTypesWithLayout(source, defaultCompilePathLayout())
}

func resolveLegacyRouteTypesWithLayout(source *shape.Source, layout compilePathLayout) []*plan.Type {
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
	yamlCandidates := legacyRouteYAMLCandidates(routesBase, stem, typeStem)
	var payload struct {
		Resource struct {
			Types []struct {
				Name        string `yaml:"Name"`
				Alias       string `yaml:"Alias"`
				DataType    string `yaml:"DataType"`
				Cardinality string `yaml:"Cardinality"`
				Package     string `yaml:"Package"`
				ModulePath  string `yaml:"ModulePath"`
			} `yaml:"Types"`
		} `yaml:"Resource"`
	}
	loaded := false
	for _, candidate := range yamlCandidates {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		if err = yaml.Unmarshal(data, &payload); err != nil {
			continue
		}
		loaded = true
		break
	}
	if !loaded || len(payload.Resource.Types) == 0 {
		return nil
	}
	result := make([]*plan.Type, 0, len(payload.Resource.Types))
	seen := map[string]bool{}
	for _, item := range payload.Resource.Types {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, &plan.Type{
			Name:        name,
			Alias:       strings.TrimSpace(item.Alias),
			DataType:    strings.TrimSpace(item.DataType),
			Cardinality: strings.TrimSpace(item.Cardinality),
			Package:     strings.TrimSpace(item.Package),
			ModulePath:  strings.TrimSpace(item.ModulePath),
		})
	}
	return result
}

func mergeLegacyRouteStates(result *plan.Result, source *shape.Source) {
	mergeLegacyRouteStatesWithLayout(result, source, defaultCompilePathLayout())
}

func mergeLegacyRouteStatesWithLayout(result *plan.Result, source *shape.Source, layout compilePathLayout) {
	if result == nil {
		return
	}
	legacy := resolveLegacyRouteStatesWithLayout(source, layout)
	if len(legacy) == 0 {
		return
	}
	existing := map[string]bool{}
	for _, item := range result.States {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(item.Name)) + "|" + strings.ToLower(strings.TrimSpace(item.Kind)) + "|" + strings.ToLower(strings.TrimSpace(item.In))
		existing[key] = true
	}
	for _, item := range legacy {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(item.Name)) + "|" + strings.ToLower(strings.TrimSpace(item.Kind)) + "|" + strings.ToLower(strings.TrimSpace(item.In))
		if existing[key] {
			continue
		}
		result.States = append(result.States, item)
		existing[key] = true
	}
}

func mergeLegacyRouteTypes(result *plan.Result, source *shape.Source) {
	mergeLegacyRouteTypesWithLayout(result, source, defaultCompilePathLayout())
}

func mergeLegacyRouteTypesWithLayout(result *plan.Result, source *shape.Source, layout compilePathLayout) {
	if result == nil {
		return
	}
	legacy := resolveLegacyRouteTypesWithLayout(source, layout)
	if len(legacy) == 0 {
		return
	}
	existing := map[string]bool{}
	for _, item := range result.Types {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		existing[strings.ToLower(strings.TrimSpace(item.Name))] = true
	}
	for _, item := range legacy {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(item.Name))
		if existing[key] {
			continue
		}
		result.Types = append(result.Types, item)
		existing[key] = true
	}
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
