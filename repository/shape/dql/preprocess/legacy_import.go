package preprocess

import (
	"path"
	"regexp"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

var (
	legacyImportBlock = regexp.MustCompile(`(?ms)^[ \t]*import\s*\((.*?)\)`)
	legacyImportLine  = regexp.MustCompile(`(?m)^[ \t]*import\s*"([^"]+)"(?:\s+alias\s+"([^"]+)")?[ \t]*$`)
	legacyImportItem  = regexp.MustCompile(`"([^"]+)"(?:\s+alias\s+"([^"]+)")?`)
)

type legacyImportRange struct {
	start int
	end   int
}

func extractLegacyTypeImports(dql string) ([]typectx.Import, []legacyImportRange, []*dqlshape.Diagnostic) {
	if strings.TrimSpace(dql) == "" {
		return nil, nil, nil
	}
	var (
		imports []typectx.Import
		ranges  []legacyImportRange
		diags   []*dqlshape.Diagnostic
	)
	inBlock := make([]bool, len(dql))

	blockMatches := legacyImportBlock.FindAllStringSubmatchIndex(dql, -1)
	for _, match := range blockMatches {
		if len(match) < 4 {
			continue
		}
		start, end := match[0], match[1]
		bodyStart, bodyEnd := match[2], match[3]
		if start < 0 || end <= start || bodyStart < 0 || bodyEnd < bodyStart || bodyEnd > len(dql) {
			continue
		}
		for i := start; i < end && i < len(inBlock); i++ {
			inBlock[i] = true
		}
		ranges = append(ranges, legacyImportRange{start: start, end: end})
		blockBody := dql[bodyStart:bodyEnd]
		itemMatches := legacyImportItem.FindAllStringSubmatchIndex(blockBody, -1)
		if len(itemMatches) == 0 {
			diags = append(diags, directiveDiagnostic(
				dqldiag.CodeDirImport,
				"invalid legacy import declaration",
				`expected: import "pkg/path.Type" or import ("pkg/path.Type" alias "x")`,
				dql,
				start,
			))
			continue
		}
		for _, item := range itemMatches {
			if len(item) < 6 {
				continue
			}
			specStart := bodyStart + item[2]
			spec := strings.TrimSpace(blockBody[item[2]:item[3]])
			alias := ""
			if item[4] >= 0 && item[5] >= 0 {
				alias = strings.TrimSpace(blockBody[item[4]:item[5]])
			}
			aImport, ok := parseLegacyImportSpec(spec, alias)
			if !ok {
				diags = append(diags, directiveDiagnostic(
					dqldiag.CodeDirImport,
					"invalid legacy import declaration",
					`expected import target with type suffix: "pkg/path.Type"`,
					dql,
					specStart,
				))
				continue
			}
			imports = append(imports, aImport)
		}
	}

	lineMatches := legacyImportLine.FindAllStringSubmatchIndex(dql, -1)
	for _, match := range lineMatches {
		if len(match) < 6 {
			continue
		}
		start, end := match[0], match[1]
		if start < 0 || end <= start || start >= len(inBlock) || inBlock[start] {
			continue
		}
		spec := strings.TrimSpace(dql[match[2]:match[3]])
		alias := ""
		if match[4] >= 0 && match[5] >= 0 {
			alias = strings.TrimSpace(dql[match[4]:match[5]])
		}
		aImport, ok := parseLegacyImportSpec(spec, alias)
		if !ok {
			diags = append(diags, directiveDiagnostic(
				dqldiag.CodeDirImport,
				"invalid legacy import declaration",
				`expected import target with type suffix: "pkg/path.Type"`,
				dql,
				start,
			))
			continue
		}
		imports = append(imports, aImport)
		ranges = append(ranges, legacyImportRange{start: start, end: end})
	}

	return uniqueTypeImports(imports), ranges, diags
}

func parseLegacyImportSpec(spec, alias string) (typectx.Import, bool) {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return typectx.Import{}, false
	}
	index := strings.LastIndex(spec, ".")
	if index <= 0 || index >= len(spec)-1 {
		return typectx.Import{}, false
	}
	pkg := strings.TrimSpace(spec[:index])
	typeName := strings.TrimSpace(spec[index+1:])
	if pkg == "" || typeName == "" {
		return typectx.Import{}, false
	}
	alias = strings.TrimSpace(alias)
	if alias == "" {
		alias = path.Base(pkg)
	}
	return typectx.Import{Alias: alias, Package: pkg}, true
}

func uniqueTypeImports(input []typectx.Import) []typectx.Import {
	if len(input) == 0 {
		return nil
	}
	seen := map[string]bool{}
	result := make([]typectx.Import, 0, len(input))
	for _, item := range input {
		pkg := strings.TrimSpace(item.Package)
		if pkg == "" {
			continue
		}
		alias := strings.TrimSpace(item.Alias)
		key := strings.ToLower(alias + "|" + pkg)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, typectx.Import{Alias: alias, Package: pkg})
	}
	return result
}
