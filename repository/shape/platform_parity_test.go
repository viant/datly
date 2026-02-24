package shape_test

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	shape "github.com/viant/datly/repository/shape"
	shapecompile "github.com/viant/datly/repository/shape/compile"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
	shapeload "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
)

type parityRule struct {
	Mode      string `yaml:"mode"`
	Namespace string `yaml:"namespace"`
	Source    string `yaml:"source"`
	Connector string `yaml:"connector,omitempty"`
}

type legacyYAML struct {
	ColumnsDiscovery *bool `yaml:"ColumnsDiscovery"`
	TypeContext      struct {
		DefaultPackage string `yaml:"DefaultPackage"`
		PackageDir     string `yaml:"PackageDir"`
		PackageName    string `yaml:"PackageName"`
		PackagePath    string `yaml:"PackagePath"`
	} `yaml:"TypeContext"`
	Resource struct {
		Views []struct {
			Name       string `yaml:"Name"`
			Table      string `yaml:"Table"`
			Mode       string `yaml:"Mode"`
			Module     string `yaml:"Module"`
			AllowNulls *bool  `yaml:"AllowNulls"`
			Connector  struct {
				Ref string `yaml:"Ref"`
			} `yaml:"Connector"`
			Schema struct {
				Cardinality string `yaml:"Cardinality"`
				DataType    string `yaml:"DataType"`
				Name        string `yaml:"Name"`
			} `yaml:"Schema"`
			Template struct {
				SourceURL string `yaml:"SourceURL"`
				Summary   *struct {
					Name string `yaml:"Name"`
					Kind string `yaml:"Kind"`
				} `yaml:"Summary"`
			} `yaml:"Template"`
			Selector struct {
				Namespace        string        `yaml:"Namespace"`
				NoLimit          *bool         `yaml:"NoLimit"`
				LimitParameter   selectorParam `yaml:"LimitParameter"`
				OffsetParameter  selectorParam `yaml:"OffsetParameter"`
				PageParameter    selectorParam `yaml:"PageParameter"`
				FieldsParameter  selectorParam `yaml:"FieldsParameter"`
				OrderByParameter selectorParam `yaml:"OrderByParameter"`
			} `yaml:"Selector"`
		} `yaml:"Views"`
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
		Types []struct {
			Name        string `yaml:"Name"`
			Alias       string `yaml:"Alias"`
			DataType    string `yaml:"DataType"`
			Cardinality string `yaml:"Cardinality"`
			Package     string `yaml:"Package"`
			ModulePath  string `yaml:"ModulePath"`
		} `yaml:"Types"`
	} `yaml:"Resource"`
	Routes []struct {
		Method string `yaml:"Method"`
		URI    string `yaml:"URI"`
		View   struct {
			Ref string `yaml:"Ref"`
		} `yaml:"View"`
	} `yaml:"Routes"`
}

type viewIR struct {
	Name      string `yaml:"name"`
	Table     string `yaml:"table"`
	Connector string `yaml:"connector,omitempty"`
	SQLURI    string `yaml:"sqlUri,omitempty"`
}

type routeIR struct {
	Method string `yaml:"method,omitempty"`
	URI    string `yaml:"uri,omitempty"`
	View   string `yaml:"view,omitempty"`
}

type resourceMetaIR struct {
	ColumnsDiscovery *bool `yaml:"columnsDiscovery,omitempty"`
}

type viewMetaIR struct {
	Name              string `yaml:"name"`
	Mode              string `yaml:"mode,omitempty"`
	Module            string `yaml:"module,omitempty"`
	AllowNulls        *bool  `yaml:"allowNulls,omitempty"`
	SelectorNamespace string `yaml:"selectorNamespace,omitempty"`
	SelectorNoLimit   *bool  `yaml:"selectorNoLimit,omitempty"`
	SchemaCardinality string `yaml:"schemaCardinality,omitempty"`
	SchemaType        string `yaml:"schemaType,omitempty"`
	HasSummary        *bool  `yaml:"hasSummary,omitempty"`
}

type parityOutput struct {
	Namespace      string                 `yaml:"namespace"`
	Source         string                 `yaml:"source"`
	LegacyYAML     string                 `yaml:"legacyYaml"`
	LegacyMeta     *resourceMetaIR        `yaml:"legacyMeta,omitempty"`
	LegacyViews    []viewIR               `yaml:"legacyViews,omitempty"`
	LegacyViewMeta []viewMetaIR           `yaml:"legacyViewMeta,omitempty"`
	LegacyParams   []paramIR              `yaml:"legacyParams,omitempty"`
	LegacyRoutes   []routeIR              `yaml:"legacyRoutes,omitempty"`
	LegacyTypes    []typeIR               `yaml:"legacyTypes,omitempty"`
	LegacyTypeCtx  *typeCtxIR             `yaml:"legacyTypeContext,omitempty"`
	ShapeMeta      *resourceMetaIR        `yaml:"shapeMeta,omitempty"`
	ShapeViews     []viewIR               `yaml:"shapeViews,omitempty"`
	ShapeViewMeta  []viewMetaIR           `yaml:"shapeViewMeta,omitempty"`
	ShapeParams    []paramIR              `yaml:"shapeParams,omitempty"`
	ShapeTypes     []typeIR               `yaml:"shapeTypes,omitempty"`
	ShapeTypeCtx   *typeCtxIR             `yaml:"shapeTypeContext,omitempty"`
	ShapeDiags     []string               `yaml:"shapeDiagnostics,omitempty"`
	Mismatches     []string               `yaml:"mismatches,omitempty"`
	CompileFailed  bool                   `yaml:"compileFailed,omitempty"`
	RawDiagnostics []*dqlshape.Diagnostic `yaml:"-"`
}

type parityReport struct {
	Total       int      `yaml:"total"`
	Compared    int      `yaml:"compared"`
	WithDiff    int      `yaml:"withDiff"`
	MissingYAML int      `yaml:"missingYaml"`
	Failures    int      `yaml:"failures"`
	TopIssues   []string `yaml:"topIssues,omitempty"`
}

type selectorParam struct {
	Name      string `yaml:"Name"`
	Cacheable *bool  `yaml:"Cacheable"`
	In        struct {
		Kind string `yaml:"Kind"`
		Name string `yaml:"Name"`
	} `yaml:"In"`
}

type paramIR struct {
	Name          string   `yaml:"name"`
	Kind          string   `yaml:"kind,omitempty"`
	In            string   `yaml:"in,omitempty"`
	Required      *bool    `yaml:"required,omitempty"`
	Cacheable     *bool    `yaml:"cacheable,omitempty"`
	URI           string   `yaml:"uri,omitempty"`
	Value         string   `yaml:"value,omitempty"`
	QuerySelector string   `yaml:"querySelector,omitempty"`
	Predicates    []string `yaml:"predicates,omitempty"`
}

type typeIR struct {
	Name        string `yaml:"name"`
	Alias       string `yaml:"alias,omitempty"`
	DataType    string `yaml:"dataType,omitempty"`
	Cardinality string `yaml:"cardinality,omitempty"`
	Package     string `yaml:"package,omitempty"`
	ModulePath  string `yaml:"modulePath,omitempty"`
}

type typeCtxIR struct {
	DefaultPackage string `yaml:"defaultPackage,omitempty"`
	PackageDir     string `yaml:"packageDir,omitempty"`
	PackageName    string `yaml:"packageName,omitempty"`
	PackagePath    string `yaml:"packagePath,omitempty"`
}

type parityEntryEval struct {
	Output            parityOutput
	SourceReadable    bool
	MissingLegacyYAML bool
}

func TestPlatform_DQLToRoute_ParityIR_SmokeHandlers(t *testing.T) {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("PLATFORM_PARITY_SMOKE")), "1") {
		t.Skip("set PLATFORM_PARITY_SMOKE=1 to run legacy parity smoke handlers")
	}
	platformRoot := os.Getenv("PLATFORM_ROOT")
	if platformRoot == "" {
		platformRoot = "/Users/awitas/go/src/github.vianttech.com/viant/platform"
	}
	rulesRoot := filepath.Join(platformRoot, "e2e", "rule")
	routesRoot := filepath.Join(platformRoot, "repo", "dev", "Datly", "routes")
	if _, err := os.Stat(rulesRoot); err != nil {
		if os.Getenv("PLATFORM_PARITY_SMOKE_REQUIRED") == "1" {
			t.Fatalf("platform rules not found at %s", rulesRoot)
		}
		t.Skipf("platform rules not found at %s", rulesRoot)
	}
	entries, err := collectRuleMappings(rulesRoot)
	if err != nil {
		t.Fatalf("collect mappings: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("no dql->route mappings found under %s", rulesRoot)
	}
	entryBySource := map[string]parityRule{}
	for _, entry := range entries {
		entryBySource[entry.Source] = entry
	}
	highRiskHandlers := collectSmokeHandlerSources(entries, routesRoot)
	if len(highRiskHandlers) < 5 {
		t.Fatalf("smoke handler discovery returned too few sources: %d", len(highRiskHandlers))
	}

	compiler := shapecompile.New()
	for _, source := range highRiskHandlers {
		entry, ok := entryBySource[source]
		if !ok {
			t.Fatalf("smoke source not found in rule mappings: %s", source)
		}
		eval := evaluateParityEntry(platformRoot, routesRoot, entry, compiler)
		if !eval.SourceReadable {
			t.Fatalf("unable to read source for smoke source: %s", source)
		}
		if eval.MissingLegacyYAML {
			t.Fatalf("missing legacy yaml for smoke source: %s", source)
		}
		out := eval.Output
		if out.CompileFailed {
			t.Fatalf("shape compile failed for %s: %v", source, out.ShapeDiags)
		}
		if len(out.Mismatches) > 0 {
			t.Fatalf("parity mismatches for %s: %v", source, out.Mismatches)
		}
	}
}

func TestPlatform_DQLToRoute_ParityIR(t *testing.T) {
	platformRoot := os.Getenv("PLATFORM_ROOT")
	if platformRoot == "" {
		platformRoot = "/Users/awitas/go/src/github.vianttech.com/viant/platform"
	}
	rulesRoot := filepath.Join(platformRoot, "e2e", "rule")
	routesRoot := filepath.Join(platformRoot, "repo", "dev", "Datly", "routes")
	if _, err := os.Stat(rulesRoot); err != nil {
		t.Skipf("platform rules not found at %s", rulesRoot)
	}
	entries, err := collectRuleMappings(rulesRoot)
	if err != nil {
		t.Fatalf("collect mappings: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("no dql->route mappings found under %s", rulesRoot)
	}
	targetSource := strings.TrimSpace(os.Getenv("PLATFORM_PARITY_SOURCE"))
	runAll := strings.EqualFold(targetSource, "all") || targetSource == "*" || strings.EqualFold(strings.TrimSpace(os.Getenv("PLATFORM_PARITY_ALL")), "1")
	if targetSource == "" && !runAll {
		t.Skip("set PLATFORM_PARITY_SOURCE to run transient platform parity check")
	}
	if !runAll {
		var filtered []parityRule
		for _, entry := range entries {
			if entry.Source == targetSource {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) == 0 {
			t.Fatalf("target source not found in rules: %s", targetSource)
		}
		entries = filtered
	}

	compiler := shapecompile.New()
	report := parityReport{Total: len(entries)}
	issueCounts := map[string]int{}

	for _, entry := range entries {
		eval := evaluateParityEntry(platformRoot, routesRoot, entry, compiler)
		if !eval.SourceReadable {
			continue
		}
		if eval.MissingLegacyYAML {
			report.MissingYAML++
			continue
		}
		report.Compared++
		out := eval.Output
		routeYAMLPath := out.LegacyYAML
		if out.CompileFailed {
			issueCounts["shape compile failed"]++
			report.Failures++
			writeIRFile(routeYAMLPath+".shape.ir.yaml", out)
			report.WithDiff++
			continue
		}
		if len(out.Mismatches) > 0 {
			report.WithDiff++
			for _, m := range out.Mismatches {
				issueCounts[m]++
			}
		}
		writeIRFile(routeYAMLPath+".shape.ir.yaml", out)
	}

	report.TopIssues = topIssues(issueCounts, 10)
	reportPath := filepath.Join(routesRoot, "_shape_parity_report.yaml")
	writeYAML(reportPath, report)
	t.Logf("parity report: %s", reportPath)
	t.Logf("total=%d compared=%d withDiff=%d missingYaml=%d failures=%d", report.Total, report.Compared, report.WithDiff, report.MissingYAML, report.Failures)
}

func collectSmokeHandlerSources(entries []parityRule, routesRoot string) []string {
	excluded := map[string]bool{}
	var result []string
	for _, entry := range entries {
		source := strings.TrimSpace(entry.Source)
		if !isHandlerLikeSource(source) {
			continue
		}
		if excluded[source] {
			continue
		}
		routeYAMLPath := filepath.Join(routesRoot, entry.Namespace, routeYAMLName(source))
		if _, err := os.Stat(routeYAMLPath); err != nil {
			continue
		}
		result = append(result, source)
	}
	sort.Strings(result)
	return dedupe(result)
}

func isHandlerLikeSource(source string) bool {
	source = strings.ToLower(strings.TrimSpace(source))
	if source == "" {
		return false
	}
	if strings.Contains(source, "/gen/") && (strings.HasSuffix(source, ".dql") || strings.HasSuffix(source, ".sql")) {
		return true
	}
	return strings.HasSuffix(source, "/patch.dql") ||
		strings.HasSuffix(source, "/patch.sql") ||
		strings.HasSuffix(source, "/post.dql") ||
		strings.HasSuffix(source, "/post.sql") ||
		strings.HasSuffix(source, "/put.dql") ||
		strings.HasSuffix(source, "/put.sql") ||
		strings.HasSuffix(source, "/delete.dql") ||
		strings.HasSuffix(source, "/delete.sql") ||
		strings.HasSuffix(source, "/upload.dql") ||
		strings.HasSuffix(source, "/upload.sql") ||
		strings.HasSuffix(source, "/export.dql") ||
		strings.HasSuffix(source, "/export.sql") ||
		strings.HasSuffix(source, "/action.dql") ||
		strings.HasSuffix(source, "/action.sql")
}

func evaluateParityEntry(platformRoot, routesRoot string, entry parityRule, compiler *shapecompile.DQLCompiler) parityEntryEval {
	sourcePath := filepath.Join(platformRoot, entry.Source)
	routeYAMLPath, _ := resolveLegacyRouteYAMLPath(routesRoot, entry.Namespace, entry.Source)
	if routeYAMLPath == "" {
		routeYAMLPath = filepath.Join(routesRoot, entry.Namespace, routeYAMLName(entry.Source))
	}
	out := parityEntryEval{Output: parityOutput{
		Namespace:  entry.Namespace,
		Source:     entry.Source,
		LegacyYAML: routeYAMLPath,
	}}
	sourceBytes, readErr := os.ReadFile(sourcePath)
	if readErr != nil {
		return out
	}
	out.SourceReadable = true
	legacyBytes, legacyErr := os.ReadFile(routeYAMLPath)
	if legacyErr != nil {
		out.MissingLegacyYAML = true
		return out
	}
	sourceName := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
	if sourceName == "" {
		sourceName = entry.Namespace
	}

	var legacy legacyYAML
	if err := yaml.Unmarshal(legacyBytes, &legacy); err == nil {
		out.Output.LegacyMeta = &resourceMetaIR{ColumnsDiscovery: legacy.ColumnsDiscovery}
		out.Output.LegacyViews = make([]viewIR, 0, len(legacy.Resource.Views))
		out.Output.LegacyViewMeta = make([]viewMetaIR, 0, len(legacy.Resource.Views))
		for _, v := range legacy.Resource.Views {
			out.Output.LegacyViews = append(out.Output.LegacyViews, viewIR{
				Name:      v.Name,
				Table:     v.Table,
				Connector: v.Connector.Ref,
				SQLURI:    v.Template.SourceURL,
			})
			var hasSummary *bool
			if v.Template.Summary != nil {
				value := true
				hasSummary = &value
			}
			out.Output.LegacyViewMeta = append(out.Output.LegacyViewMeta, viewMetaIR{
				Name:              strings.TrimSpace(v.Name),
				Mode:              strings.TrimSpace(v.Mode),
				Module:            strings.TrimSpace(v.Module),
				AllowNulls:        v.AllowNulls,
				SelectorNamespace: strings.TrimSpace(v.Selector.Namespace),
				SelectorNoLimit:   v.Selector.NoLimit,
				SchemaCardinality: strings.TrimSpace(v.Schema.Cardinality),
				SchemaType:        firstNonEmpty(strings.TrimSpace(v.Schema.DataType), strings.TrimSpace(v.Schema.Name)),
				HasSummary:        hasSummary,
			})
		}
		for _, r := range legacy.Routes {
			out.Output.LegacyRoutes = append(out.Output.LegacyRoutes, routeIR{
				Method: r.Method,
				URI:    r.URI,
				View:   r.View.Ref,
			})
		}
		out.Output.LegacyTypeCtx = normalizeTypeContextIR(
			legacy.TypeContext.DefaultPackage,
			legacy.TypeContext.PackageDir,
			legacy.TypeContext.PackageName,
			legacy.TypeContext.PackagePath,
		)
		out.Output.LegacyParams = normalizeLegacyParams(legacy)
		out.Output.LegacyTypes = normalizeLegacyTypes(legacy)
	}

	planResult, compileErr := compiler.Compile(context.Background(), &shape.Source{
		Name:      sourceName,
		Path:      sourcePath,
		Connector: entry.Connector,
		DQL:       string(sourceBytes),
	})
	if compileErr != nil {
		out.Output.CompileFailed = true
		if cErr, ok := compileErr.(*shapecompile.CompileError); ok {
			out.Output.RawDiagnostics = cErr.Diagnostics
			for _, d := range cErr.Diagnostics {
				if d == nil {
					continue
				}
				out.Output.ShapeDiags = append(out.Output.ShapeDiags, d.Error())
			}
		} else {
			out.Output.ShapeDiags = append(out.Output.ShapeDiags, compileErr.Error())
		}
		out.Output.Mismatches = append(out.Output.Mismatches, "shape compile failed")
		return out
	}

	planned, _ := planResult.Plan.(*plan.Result)
	if planned != nil {
		out.Output.ShapeMeta = &resourceMetaIR{}
		if sourcePath != "" {
			value := true
			out.Output.ShapeMeta.ColumnsDiscovery = &value
		}
		out.Output.ShapeViews = make([]viewIR, 0, len(planned.Views))
		out.Output.ShapeViewMeta = make([]viewMetaIR, 0, len(planned.Views))
		for _, v := range planned.Views {
			if v == nil {
				continue
			}
			out.Output.ShapeViews = append(out.Output.ShapeViews, viewIR{
				Name:      v.Name,
				Table:     v.Table,
				Connector: v.Connector,
				SQLURI:    v.SQLURI,
			})
			var hasSummary *bool
			if strings.TrimSpace(v.Summary) != "" {
				value := true
				hasSummary = &value
			}
			out.Output.ShapeViewMeta = append(out.Output.ShapeViewMeta, viewMetaIR{
				Name:              strings.TrimSpace(v.Name),
				Mode:              inferShapeViewMode(v.SQL),
				Module:            strings.TrimSpace(v.Module),
				AllowNulls:        v.AllowNulls,
				SelectorNamespace: strings.TrimSpace(v.SelectorNamespace),
				SelectorNoLimit:   v.SelectorNoLimit,
				SchemaCardinality: normalizeCardinality(strings.TrimSpace(v.Cardinality)),
				SchemaType:        strings.TrimSpace(v.SchemaType),
				HasSummary:        hasSummary,
			})
		}
		for _, d := range planned.Diagnostics {
			if d == nil {
				continue
			}
			out.Output.ShapeDiags = append(out.Output.ShapeDiags, d.Error())
		}
		loader := shapeload.New()
		if artifacts, err := loader.LoadViews(context.Background(), planResult); err == nil && artifacts != nil && artifacts.Resource != nil {
			mergeShapeViewMetadata(out.Output.ShapeViewMeta, artifacts.Resource.Views)
		}
		out.Output.ShapeParams = normalizeShapeParams(planned)
		out.Output.ShapeTypes = normalizeShapeTypes(planned, sourcePath)
		if planned.TypeContext != nil {
			out.Output.ShapeTypeCtx = normalizeTypeContextIR(
				planned.TypeContext.DefaultPackage,
				planned.TypeContext.PackageDir,
				planned.TypeContext.PackageName,
				planned.TypeContext.PackagePath,
			)
		}
	}

	out.Output.Mismatches = compareParity(out.Output.LegacyViews, out.Output.ShapeViews)
	out.Output.Mismatches = append(out.Output.Mismatches, compareMetadataParity(out.Output.LegacyMeta, out.Output.ShapeMeta, out.Output.LegacyViewMeta, out.Output.ShapeViewMeta)...)
	out.Output.Mismatches = append(out.Output.Mismatches, compareParamParity(out.Output.LegacyParams, out.Output.ShapeParams)...)
	out.Output.Mismatches = append(out.Output.Mismatches, compareTypeParity(out.Output.LegacyTypes, out.Output.ShapeTypes)...)
	out.Output.Mismatches = append(out.Output.Mismatches, compareTypeContextParity(out.Output.LegacyTypeCtx, out.Output.ShapeTypeCtx)...)
	out.Output.Mismatches = dedupe(out.Output.Mismatches)
	return out
}

func resolveLegacyRouteYAMLPath(routesRoot, namespace, source string) (string, bool) {
	candidates := legacyRouteYAMLCandidatePaths(routesRoot, namespace, source)
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, true
		}
	}
	return "", false
}

func legacyRouteYAMLCandidatePaths(routesRoot, namespace, source string) []string {
	namespace = strings.Trim(strings.TrimSpace(namespace), "/")
	stem := strings.TrimSuffix(filepath.Base(strings.TrimSpace(source)), filepath.Ext(strings.TrimSpace(source)))
	if stem == "" {
		stem = "route"
	}
	fileName := stem + ".yaml"
	nsPath := filepath.FromSlash(namespace)
	leaf := filepath.Base(nsPath)
	parent := filepath.Dir(nsPath)

	appendUnique := func(items *[]string, seen map[string]bool, path string) {
		path = filepath.Clean(path)
		if path == "." || path == "" || seen[path] {
			return
		}
		seen[path] = true
		*items = append(*items, path)
	}

	seen := map[string]bool{}
	result := make([]string, 0, 8)
	appendUnique(&result, seen, filepath.Join(routesRoot, nsPath, fileName))
	appendUnique(&result, seen, filepath.Join(routesRoot, nsPath, stem, fileName))
	if leaf != "" && leaf != "." {
		appendUnique(&result, seen, filepath.Join(routesRoot, nsPath, leaf+".yaml"))
	}
	if parent != "" && parent != "." {
		appendUnique(&result, seen, filepath.Join(routesRoot, parent, fileName))
		appendUnique(&result, seen, filepath.Join(routesRoot, parent, stem, fileName))
		parentLeaf := filepath.Base(parent)
		if parentLeaf != "" && parentLeaf != "." {
			appendUnique(&result, seen, filepath.Join(routesRoot, parent, parentLeaf+".yaml"))
		}
	}
	if strings.Contains(strings.ToLower(source), "/gen/") {
		appendUnique(&result, seen, filepath.Join(routesRoot, nsPath, "patch", "patch.yaml"))
	}
	return result
}

func collectRuleMappings(rulesRoot string) ([]parityRule, error) {
	var files []string
	if err := filepath.WalkDir(rulesRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		files = append(files, path)
		return nil
	}); err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`\$appPath/bin/datly\s+(gen|translate)\s+.*-u=([^\s]+)\s+-s='([^']+)'(.*)`)
	seen := map[string]bool{}
	var result []parityRule
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		lines := strings.Split(string(data), "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			m := re.FindStringSubmatch(line)
			if len(m) < 4 {
				continue
			}
			src := strings.TrimSpace(m[3])
			if !(strings.HasSuffix(src, ".dql") || strings.HasSuffix(src, ".sql")) {
				continue
			}
			connector := inferRuleConnector("")
			if len(m) >= 5 {
				connector = inferRuleConnector(m[4])
			}
			key := m[2] + "|" + src
			if seen[key] {
				continue
			}
			seen[key] = true
			result = append(result, parityRule{
				Mode:      strings.TrimSpace(m[1]),
				Namespace: strings.TrimSpace(m[2]),
				Source:    src,
				Connector: connector,
			})
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Namespace == result[j].Namespace {
			return result[i].Source < result[j].Source
		}
		return result[i].Namespace < result[j].Namespace
	})
	return result, nil
}

func inferRuleConnector(tail string) string {
	lower := strings.ToLower(tail)
	switch {
	case strings.Contains(lower, "$optionsaero"):
		return "system"
	case strings.Contains(lower, "$optionssitemgmt"):
		return "sitemgmt"
	case strings.Contains(lower, "$options"):
		return "ci_ads"
	default:
		return ""
	}
}

func routeYAMLName(source string) string {
	base := filepath.Base(source)
	ext := filepath.Ext(base)
	return strings.TrimSuffix(base, ext) + ".yaml"
}

func compareParity(legacy, shapeViews []viewIR) []string {
	var result []string
	if len(legacy) != len(shapeViews) {
		result = append(result, "view count mismatch")
	}
	legacyByName := map[string]viewIR{}
	for _, v := range legacy {
		legacyByName[strings.ToLower(v.Name)] = v
	}
	for _, s := range shapeViews {
		l, ok := legacyByName[strings.ToLower(s.Name)]
		if !ok {
			result = append(result, "missing view in legacy: "+s.Name)
			continue
		}
		if l.Table != "" && s.Table != "" && !strings.EqualFold(l.Table, s.Table) {
			result = append(result, "table mismatch: "+s.Name)
		}
		if l.Connector != "" && s.Connector == "" {
			result = append(result, "connector missing in shape: "+s.Name)
		}
		if l.Connector != "" && s.Connector != "" && !strings.EqualFold(strings.TrimSpace(l.Connector), strings.TrimSpace(s.Connector)) {
			result = append(result, "connector mismatch: "+s.Name)
		}
		if l.SQLURI != "" && s.SQLURI == "" {
			result = append(result, "sql uri missing in shape: "+s.Name)
		}
		if l.SQLURI != "" && s.SQLURI != "" && !equalSQLURI(l.SQLURI, s.SQLURI) {
			result = append(result, "sql uri mismatch: "+s.Name)
		}
	}
	return dedupe(result)
}

func equalSQLURI(legacy, shape string) bool {
	normalize := func(v string) string {
		v = strings.ReplaceAll(strings.TrimSpace(v), "\\", "/")
		return strings.TrimPrefix(v, "./")
	}
	return strings.EqualFold(normalize(legacy), normalize(shape))
}

func normalizeCardinality(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "one":
		return "One"
	case "many":
		return "Many"
	default:
		return strings.TrimSpace(value)
	}
}

func inferShapeViewMode(sql string) string {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return ""
	}
	statements := dqlstmt.New(sql)
	hasRead := false
	hasExec := false
	for _, item := range statements {
		if item == nil {
			continue
		}
		switch item.Kind {
		case dqlstmt.KindRead:
			hasRead = true
		case dqlstmt.KindExec:
			hasExec = true
		}
	}
	switch {
	case hasRead && !hasExec:
		return "SQLQuery"
	case hasExec && !hasRead:
		return "SQLExec"
	case hasRead && hasExec:
		return "SQLExec"
	}
	stmt := strings.ToLower(sql)
	if strings.HasPrefix(stmt, "select") || strings.HasPrefix(stmt, "with") {
		return "SQLQuery"
	}
	return ""
}

func mergeShapeViewMetadata(meta []viewMetaIR, views view.Views) {
	if len(meta) == 0 || len(views) == 0 {
		return
	}
	index := map[string]int{}
	for i, item := range meta {
		index[strings.ToLower(strings.TrimSpace(item.Name))] = i
	}
	for _, candidate := range views {
		if candidate == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(candidate.Name))
		pos, ok := index[key]
		if !ok {
			continue
		}
		if mode := strings.TrimSpace(string(candidate.Mode)); mode != "" {
			meta[pos].Mode = mode
		}
		if meta[pos].Module == "" {
			meta[pos].Module = strings.TrimSpace(candidate.Module)
		}
		if meta[pos].AllowNulls == nil {
			meta[pos].AllowNulls = candidate.AllowNulls
		}
		if candidate.Selector != nil {
			if meta[pos].SelectorNamespace == "" {
				meta[pos].SelectorNamespace = strings.TrimSpace(candidate.Selector.Namespace)
			}
			if meta[pos].SelectorNoLimit == nil {
				meta[pos].SelectorNoLimit = &candidate.Selector.NoLimit
			}
		}
		if candidate.Schema != nil {
			if meta[pos].SchemaCardinality == "" {
				meta[pos].SchemaCardinality = strings.TrimSpace(string(candidate.Schema.Cardinality))
			}
			if meta[pos].SchemaType == "" {
				meta[pos].SchemaType = firstNonEmpty(strings.TrimSpace(candidate.Schema.DataType), strings.TrimSpace(candidate.Schema.Name))
			}
		}
		if candidate.Template != nil && candidate.Template.Summary != nil {
			value := true
			meta[pos].HasSummary = &value
		}
	}
}

func compareMetadataParity(legacyMeta, shapeMeta *resourceMetaIR, legacyViews, shapeViews []viewMetaIR) []string {
	var result []string
	if legacyMeta != nil && legacyMeta.ColumnsDiscovery != nil {
		if shapeMeta == nil || shapeMeta.ColumnsDiscovery == nil {
			result = append(result, "resource columnsDiscovery missing in shape")
		} else if *legacyMeta.ColumnsDiscovery != *shapeMeta.ColumnsDiscovery {
			result = append(result, "resource columnsDiscovery mismatch")
		}
	}
	legacyByName := map[string]viewMetaIR{}
	for _, item := range legacyViews {
		legacyByName[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}
	for _, shapeItem := range shapeViews {
		key := strings.ToLower(strings.TrimSpace(shapeItem.Name))
		legacyItem, ok := legacyByName[key]
		if !ok {
			continue
		}
		if legacyItem.Mode != "" {
			if shapeItem.Mode == "" {
				result = append(result, "view mode missing in shape: "+shapeItem.Name)
			} else if !strings.EqualFold(legacyItem.Mode, shapeItem.Mode) {
				result = append(result, "view mode mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.Module != "" {
			if shapeItem.Module == "" {
				result = append(result, "view module missing in shape: "+shapeItem.Name)
			} else if !strings.EqualFold(strings.TrimSpace(legacyItem.Module), strings.TrimSpace(shapeItem.Module)) {
				result = append(result, "view module mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.AllowNulls != nil {
			if shapeItem.AllowNulls == nil {
				result = append(result, "view allowNulls missing in shape: "+shapeItem.Name)
			} else if *legacyItem.AllowNulls != *shapeItem.AllowNulls {
				result = append(result, "view allowNulls mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.SelectorNamespace != "" {
			if shapeItem.SelectorNamespace == "" {
				result = append(result, "view selector namespace missing in shape: "+shapeItem.Name)
			} else if !strings.EqualFold(strings.TrimSpace(legacyItem.SelectorNamespace), strings.TrimSpace(shapeItem.SelectorNamespace)) {
				result = append(result, "view selector namespace mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.SelectorNoLimit != nil {
			if shapeItem.SelectorNoLimit == nil {
				result = append(result, "view selector noLimit missing in shape: "+shapeItem.Name)
			} else if *legacyItem.SelectorNoLimit != *shapeItem.SelectorNoLimit {
				result = append(result, "view selector noLimit mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.SchemaCardinality != "" {
			if shapeItem.SchemaCardinality == "" {
				result = append(result, "view schema cardinality missing in shape: "+shapeItem.Name)
			} else if !strings.EqualFold(strings.TrimSpace(legacyItem.SchemaCardinality), strings.TrimSpace(shapeItem.SchemaCardinality)) {
				result = append(result, "view schema cardinality mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.SchemaType != "" {
			if shapeItem.SchemaType == "" {
				result = append(result, "view schema type missing in shape: "+shapeItem.Name)
			} else if !strings.EqualFold(strings.TrimSpace(legacyItem.SchemaType), strings.TrimSpace(shapeItem.SchemaType)) {
				result = append(result, "view schema type mismatch: "+shapeItem.Name)
			}
		}
		if legacyItem.HasSummary != nil {
			if shapeItem.HasSummary == nil {
				result = append(result, "view template summary missing in shape: "+shapeItem.Name)
			} else if *legacyItem.HasSummary != *shapeItem.HasSummary {
				result = append(result, "view template summary mismatch: "+shapeItem.Name)
			}
		}
	}
	return dedupe(result)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeLegacyParams(legacy legacyYAML) []paramIR {
	querySelectors := map[string]string{}
	querySelectorCacheable := map[string]*bool{}
	querySelectorIn := map[string]string{}
	for _, v := range legacy.Resource.Views {
		viewName := strings.TrimSpace(v.Name)
		for _, param := range []selectorParam{v.Selector.LimitParameter, v.Selector.OffsetParameter, v.Selector.PageParameter, v.Selector.FieldsParameter, v.Selector.OrderByParameter} {
			name := strings.TrimSpace(param.Name)
			if name == "" || viewName == "" {
				continue
			}
			querySelectors[strings.ToLower(name)] = viewName
			querySelectorIn[strings.ToLower(name)] = strings.TrimSpace(param.In.Name)
			if param.Cacheable != nil {
				value := *param.Cacheable
				querySelectorCacheable[strings.ToLower(name)] = &value
			}
		}
	}
	result := make([]paramIR, 0, len(legacy.Resource.Parameters))
	seen := map[string]bool{}
	for _, p := range legacy.Resource.Parameters {
		name := strings.TrimSpace(p.Name)
		item := paramIR{
			Name:      name,
			Kind:      strings.TrimSpace(p.In.Kind),
			In:        strings.TrimSpace(p.In.Name),
			Required:  p.Required,
			Cacheable: p.Cacheable,
			URI:       strings.TrimSpace(p.URI),
			Value:     strings.TrimSpace(p.Value),
		}
		if selector, ok := querySelectors[strings.ToLower(name)]; ok {
			item.QuerySelector = selector
			if item.Cacheable == nil {
				item.Cacheable = querySelectorCacheable[strings.ToLower(name)]
			}
		}
		for _, pred := range p.Predicates {
			item.Predicates = append(item.Predicates, normalizePredicateSig(pred.Group, pred.Name, pred.Ensure, pred.Args))
		}
		sort.Strings(item.Predicates)
		result = append(result, item)
		seen[strings.ToLower(name)] = true
	}
	for key, selector := range querySelectors {
		if seen[key] {
			continue
		}
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		legacyName := name
		for _, v := range legacy.Resource.Views {
			for _, param := range []selectorParam{v.Selector.LimitParameter, v.Selector.OffsetParameter, v.Selector.PageParameter, v.Selector.FieldsParameter, v.Selector.OrderByParameter} {
				if strings.EqualFold(strings.TrimSpace(param.Name), key) {
					legacyName = strings.TrimSpace(param.Name)
					break
				}
			}
		}
		result = append(result, paramIR{
			Name:          legacyName,
			Kind:          "query",
			In:            strings.TrimSpace(querySelectorIn[key]),
			QuerySelector: selector,
			Cacheable:     querySelectorCacheable[key],
		})
	}
	sort.Slice(result, func(i, j int) bool {
		if strings.EqualFold(result[i].Name, result[j].Name) {
			if strings.EqualFold(result[i].Kind, result[j].Kind) {
				return strings.ToLower(result[i].In) < strings.ToLower(result[j].In)
			}
			return strings.ToLower(result[i].Kind) < strings.ToLower(result[j].Kind)
		}
		return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
	})
	return result
}

func normalizeLegacyTypes(legacy legacyYAML) []typeIR {
	if len(legacy.Resource.Types) == 0 {
		return nil
	}
	result := make([]typeIR, 0, len(legacy.Resource.Types))
	seen := map[string]bool{}
	for _, item := range legacy.Resource.Types {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, typeIR{
			Name:        name,
			Alias:       strings.TrimSpace(item.Alias),
			DataType:    normalizeTypeSignature(item.DataType),
			Cardinality: normalizeCardinality(strings.TrimSpace(item.Cardinality)),
			Package:     strings.TrimSpace(item.Package),
			ModulePath:  strings.TrimSpace(item.ModulePath),
		})
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
	})
	return result
}

func normalizeShapeTypes(planned *plan.Result, sourcePath string) []typeIR {
	if planned == nil {
		return nil
	}
	modulePrefix := inferModulePrefix(sourcePath)
	typeImportByAlias, typeImportByPkg := typeImports(planned)
	byName := map[string]typeIR{}

	register := func(item typeIR, overwrite bool) {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			return
		}
		key := strings.ToLower(name)
		if existing, ok := byName[key]; ok {
			if (overwrite || existing.DataType == "") && item.DataType != "" {
				existing.DataType = item.DataType
			}
			if (overwrite || existing.Cardinality == "") && item.Cardinality != "" {
				existing.Cardinality = item.Cardinality
			}
			if (overwrite || existing.Package == "") && item.Package != "" {
				existing.Package = item.Package
			}
			if (overwrite || existing.ModulePath == "") && item.ModulePath != "" {
				existing.ModulePath = item.ModulePath
			}
			if overwrite && item.Alias != "" {
				existing.Alias = item.Alias
			}
			byName[key] = existing
			return
		}
		byName[key] = item
	}

	for _, item := range planned.Views {
		if item == nil {
			continue
		}
		dataType := strings.TrimSpace(item.SchemaType)
		name := typeNameFromDataType(dataType)
		if name == "" && item.ElementType != nil {
			name = strings.TrimSpace(item.ElementType.Name())
			if dataType == "" && name != "" {
				dataType = "*" + name
			}
		}
		if name == "" {
			continue
		}
		pkg := packageFromDataType(dataType)
		modulePath := ""
		if strings.TrimSpace(item.Module) != "" && modulePrefix != "" {
			modulePath = modulePrefix + strings.Trim(strings.TrimSpace(item.Module), "/")
		}
		if modulePath == "" && pkg != "" {
			modulePath = firstNonEmpty(typeImportByAlias[strings.ToLower(pkg)], typeImportByPkg[strings.ToLower(pkg)])
		}
		register(typeIR{
			Name:        name,
			DataType:    normalizeTypeSignature(dataType),
			Cardinality: normalizeCardinality(strings.TrimSpace(item.Cardinality)),
			Package:     pkg,
			ModulePath:  modulePath,
		}, false)
	}

	for _, item := range planned.States {
		if item == nil || strings.TrimSpace(item.DataType) == "" {
			continue
		}
		dataType := strings.TrimSpace(item.DataType)
		name := typeNameFromDataType(dataType)
		if name == "" {
			continue
		}
		pkg := packageFromDataType(dataType)
		modulePath := firstNonEmpty(typeImportByAlias[strings.ToLower(pkg)], typeImportByPkg[strings.ToLower(pkg)])
		register(typeIR{
			Name:       name,
			DataType:   normalizeTypeSignature(dataType),
			Package:    pkg,
			ModulePath: modulePath,
		}, false)
	}
	for _, item := range planned.Types {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		register(typeIR{
			Name:        strings.TrimSpace(item.Name),
			Alias:       strings.TrimSpace(item.Alias),
			DataType:    normalizeTypeSignature(item.DataType),
			Cardinality: normalizeCardinality(strings.TrimSpace(item.Cardinality)),
			Package:     strings.TrimSpace(item.Package),
			ModulePath:  strings.TrimSpace(item.ModulePath),
		}, true)
	}

	result := make([]typeIR, 0, len(byName))
	for _, item := range byName {
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
	})
	return result
}

func compareTypeParity(legacy, shapeTypes []typeIR) []string {
	var result []string
	if len(legacy) == 0 {
		return nil
	}
	shapeByName := map[string]typeIR{}
	for _, item := range shapeTypes {
		shapeByName[strings.ToLower(strings.TrimSpace(item.Name))] = item
	}
	for _, legacyType := range legacy {
		key := strings.ToLower(strings.TrimSpace(legacyType.Name))
		shapeType, ok := shapeByName[key]
		if !ok {
			result = append(result, "missing type in shape: "+legacyType.Name)
			continue
		}
		if legacyType.DataType != "" && shapeType.DataType != "" && legacyType.DataType != shapeType.DataType {
			result = append(result, "type dataType mismatch: "+legacyType.Name)
		}
		if legacyType.Cardinality != "" && shapeType.Cardinality != "" && !strings.EqualFold(legacyType.Cardinality, shapeType.Cardinality) {
			result = append(result, "type cardinality mismatch: "+legacyType.Name)
		}
		if legacyType.Package != "" && shapeType.Package != "" && !strings.EqualFold(legacyType.Package, shapeType.Package) {
			result = append(result, "type package mismatch: "+legacyType.Name)
		}
		if legacyType.ModulePath != "" && shapeType.ModulePath != "" && !strings.EqualFold(legacyType.ModulePath, shapeType.ModulePath) {
			result = append(result, "type module path mismatch: "+legacyType.Name)
		}
		if legacyType.Alias != "" && shapeType.Alias != "" && !strings.EqualFold(legacyType.Alias, shapeType.Alias) {
			result = append(result, "type alias mismatch: "+legacyType.Name)
		}
	}
	return dedupe(result)
}

func normalizeTypeContextIR(defaultPackage, packageDir, packageName, packagePath string) *typeCtxIR {
	ret := &typeCtxIR{
		DefaultPackage: strings.TrimSpace(defaultPackage),
		PackageDir:     strings.TrimSpace(packageDir),
		PackageName:    strings.TrimSpace(packageName),
		PackagePath:    strings.TrimSpace(packagePath),
	}
	if ret.DefaultPackage == "" && ret.PackageDir == "" && ret.PackageName == "" && ret.PackagePath == "" {
		return nil
	}
	return ret
}

func compareTypeContextParity(legacy, shape *typeCtxIR) []string {
	if legacy == nil {
		return nil
	}
	if shape == nil {
		return []string{"missing type context in shape"}
	}
	var result []string
	if legacy.DefaultPackage != "" && shape.DefaultPackage != "" && !strings.EqualFold(legacy.DefaultPackage, shape.DefaultPackage) {
		result = append(result, "type context default package mismatch")
	}
	if legacy.PackageDir != "" && shape.PackageDir != "" && !strings.EqualFold(legacy.PackageDir, shape.PackageDir) {
		result = append(result, "type context package dir mismatch")
	}
	if legacy.PackageName != "" && shape.PackageName != "" && !strings.EqualFold(legacy.PackageName, shape.PackageName) {
		result = append(result, "type context package name mismatch")
	}
	if legacy.PackagePath != "" && shape.PackagePath != "" && !strings.EqualFold(legacy.PackagePath, shape.PackagePath) {
		result = append(result, "type context package path mismatch")
	}
	return dedupe(result)
}

func normalizeTypeSignature(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	parts := strings.Fields(value)
	return strings.Join(parts, " ")
}

func typeNameFromDataType(dataType string) string {
	dataType = strings.TrimSpace(dataType)
	if dataType == "" {
		return ""
	}
	dataType = strings.TrimLeft(dataType, "*[]")
	if dataType == "" {
		return ""
	}
	if idx := strings.LastIndex(dataType, "."); idx != -1 {
		dataType = dataType[idx+1:]
	}
	if idx := strings.Index(dataType, "{"); idx != -1 {
		dataType = dataType[:idx]
	}
	return strings.TrimSpace(dataType)
}

func packageFromDataType(dataType string) string {
	dataType = strings.TrimSpace(dataType)
	dataType = strings.TrimLeft(dataType, "*[]")
	if idx := strings.LastIndex(dataType, "."); idx != -1 {
		return strings.TrimSpace(dataType[:idx])
	}
	return ""
}

func inferModulePrefix(sourcePath string) string {
	normalized := filepath.ToSlash(strings.TrimSpace(sourcePath))
	if normalized == "" {
		return ""
	}
	const marker = "/src/"
	idx := strings.Index(normalized, marker)
	if idx == -1 {
		return ""
	}
	root := normalized[idx+len(marker):]
	if slash := strings.Index(root, "/dql/"); slash != -1 {
		root = root[:slash]
	}
	root = strings.Trim(root, "/")
	if root == "" {
		return ""
	}
	return root + "/pkg/"
}

func typeImports(planned *plan.Result) (map[string]string, map[string]string) {
	byAlias := map[string]string{}
	byPkg := map[string]string{}
	if planned == nil || planned.TypeContext == nil {
		return byAlias, byPkg
	}
	appendPkg := func(pkg string) {
		pkg = strings.TrimSpace(pkg)
		if pkg == "" {
			return
		}
		base := pkg
		if idx := strings.LastIndex(base, "/"); idx != -1 {
			base = base[idx+1:]
		}
		base = strings.ToLower(strings.TrimSpace(base))
		if base != "" {
			byPkg[base] = pkg
		}
	}
	if packagePath := strings.TrimSpace(planned.TypeContext.PackagePath); packagePath != "" {
		appendPkg(packagePath)
		if pkgName := strings.ToLower(strings.TrimSpace(planned.TypeContext.PackageName)); pkgName != "" {
			byAlias[pkgName] = packagePath
			byPkg[pkgName] = packagePath
		}
	}
	appendPkg(planned.TypeContext.DefaultPackage)
	for _, item := range planned.TypeContext.Imports {
		pkg := strings.TrimSpace(item.Package)
		if pkg == "" {
			continue
		}
		if alias := strings.ToLower(strings.TrimSpace(item.Alias)); alias != "" {
			byAlias[alias] = pkg
		}
		appendPkg(pkg)
	}
	return byAlias, byPkg
}

func normalizeShapeParams(planned *plan.Result) []paramIR {
	if planned == nil || len(planned.States) == 0 {
		return nil
	}
	result := make([]paramIR, 0, len(planned.States))
	for _, s := range planned.States {
		if s == nil {
			continue
		}
		item := paramIR{
			Name:          strings.TrimSpace(s.Name),
			Kind:          strings.TrimSpace(s.Kind),
			In:            strings.TrimSpace(s.In),
			Required:      s.Required,
			Cacheable:     s.Cacheable,
			URI:           strings.TrimSpace(s.URI),
			Value:         strings.TrimSpace(s.Value),
			QuerySelector: strings.TrimSpace(s.QuerySelector),
		}
		for _, pred := range s.Predicates {
			if pred == nil {
				continue
			}
			item.Predicates = append(item.Predicates, normalizePredicateSig(pred.Group, pred.Name, pred.Ensure, pred.Arguments))
		}
		sort.Strings(item.Predicates)
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool {
		if strings.EqualFold(result[i].Name, result[j].Name) {
			if strings.EqualFold(result[i].Kind, result[j].Kind) {
				return strings.ToLower(result[i].In) < strings.ToLower(result[j].In)
			}
			return strings.ToLower(result[i].Kind) < strings.ToLower(result[j].Kind)
		}
		return strings.ToLower(result[i].Name) < strings.ToLower(result[j].Name)
	})
	return result
}

func normalizePredicateSig(group int, name string, ensure bool, args []string) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, strings.TrimSpace(arg))
	}
	return strings.ToLower(strings.TrimSpace(name)) + "|" + strconv.Itoa(group) + "|" + strconv.FormatBool(ensure) + "|" + strings.Join(parts, ",")
}

func compareParamParity(legacy, shapeParams []paramIR) []string {
	var result []string
	legacyByKey := map[string]paramIR{}
	for _, item := range filterComparableParams(legacy) {
		legacyByKey[paramKey(item)] = item
	}
	shapeByKey := map[string]paramIR{}
	for _, item := range filterComparableParams(shapeParams) {
		shapeByKey[paramKey(item)] = item
	}
	if len(legacyByKey) != len(shapeByKey) {
		result = append(result, "parameter count mismatch")
	}
	for key, legacyItem := range legacyByKey {
		shapeItem, ok := shapeByKey[key]
		if !ok {
			result = append(result, "missing parameter in shape: "+legacyItem.Name)
			continue
		}
		if legacyItem.Required != nil && shapeItem.Required != nil && *legacyItem.Required != *shapeItem.Required {
			result = append(result, "parameter required mismatch: "+legacyItem.Name)
		}
		if legacyItem.Cacheable != nil && shapeItem.Cacheable != nil && *legacyItem.Cacheable != *shapeItem.Cacheable {
			result = append(result, "parameter cacheable mismatch: "+legacyItem.Name)
		}
		if legacyItem.QuerySelector != "" && !strings.EqualFold(legacyItem.QuerySelector, shapeItem.QuerySelector) {
			result = append(result, "parameter query selector mismatch: "+legacyItem.Name)
		}
		if legacyItem.URI != "" && !strings.EqualFold(strings.TrimSpace(legacyItem.URI), strings.TrimSpace(shapeItem.URI)) {
			result = append(result, "parameter uri mismatch: "+legacyItem.Name)
		}
		if len(legacyItem.Predicates) != len(shapeItem.Predicates) {
			result = append(result, "parameter predicates count mismatch: "+legacyItem.Name)
			continue
		}
		for i := range legacyItem.Predicates {
			if legacyItem.Predicates[i] != shapeItem.Predicates[i] {
				result = append(result, "parameter predicate mismatch: "+legacyItem.Name)
				break
			}
		}
	}
	return dedupe(result)
}

func paramKey(item paramIR) string {
	kind := strings.ToLower(strings.TrimSpace(item.Kind))
	in := strings.ToLower(strings.TrimSpace(item.In))
	if kind == "component" {
		in = normalizeComponentRef(in)
	}
	return strings.ToLower(strings.TrimSpace(item.Name)) + "|" + kind + "|" + in
}

func normalizeComponentRef(in string) string {
	in = strings.TrimSpace(strings.TrimPrefix(in, "get:"))
	if in == "" {
		return in
	}
	in = strings.TrimPrefix(in, "../")
	in = strings.TrimPrefix(in, "./")
	in = strings.TrimPrefix(in, "/")
	if idx := strings.LastIndex(in, "/"); idx != -1 {
		return in[idx+1:]
	}
	return in
}

func filterComparableParams(items []paramIR) []paramIR {
	if len(items) == 0 {
		return nil
	}
	result := make([]paramIR, 0, len(items))
	for _, item := range items {
		kind := strings.ToLower(strings.TrimSpace(item.Kind))
		switch kind {
		case "output", "meta", "async":
			continue
		default:
			result = append(result, item)
		}
	}
	return result
}

func dedupe(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := map[string]bool{}
	var ret []string
	for _, item := range items {
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		ret = append(ret, item)
	}
	sort.Strings(ret)
	return ret
}

func topIssues(counter map[string]int, limit int) []string {
	type pair struct {
		Issue string
		Count int
	}
	var list []pair
	for issue, count := range counter {
		list = append(list, pair{Issue: issue, Count: count})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].Count == list[j].Count {
			return list[i].Issue < list[j].Issue
		}
		return list[i].Count > list[j].Count
	})
	if len(list) > limit {
		list = list[:limit]
	}
	var ret []string
	for _, item := range list {
		ret = append(ret, item.Issue)
	}
	return ret
}

func writeIRFile(path string, v parityOutput) {
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	writeYAML(path, v)
}

func writeYAML(path string, v interface{}) {
	data, err := yaml.Marshal(v)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o644)
}
