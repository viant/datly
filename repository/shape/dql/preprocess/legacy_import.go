package preprocess

import (
	"path"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

type legacyImportRange struct {
	start int
	end   int
}

type legacyImportBlockSpec struct {
	start     int
	end       int
	bodyStart int
	bodyEnd   int
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

	blocks := findLegacyImportBlocks(dql)
	for _, block := range blocks {
		start, end := block.start, block.end
		for i := start; i < end && i < len(inBlock); i++ {
			inBlock[i] = true
		}
		ranges = append(ranges, legacyImportRange{start: start, end: end})
		blockBody := dql[block.bodyStart:block.bodyEnd]
		items := parseLegacyImportItems(blockBody, block.bodyStart)
		if len(items) == 0 {
			diags = append(diags, directiveDiagnostic(
				dqldiag.CodeDirImport,
				"invalid legacy import declaration",
				`expected: import "pkg/path.Type" or import ("pkg/path.Type" alias "x")`,
				dql,
				start,
			))
			continue
		}
		for _, item := range items {
			aImport, ok := parseLegacyImportSpec(item.spec, item.alias)
			if !ok {
				diags = append(diags, directiveDiagnostic(
					dqldiag.CodeDirImport,
					"invalid legacy import declaration",
					`expected import target with type suffix: "pkg/path.Type"`,
					dql,
					item.offset,
				))
				continue
			}
			imports = append(imports, aImport)
		}
	}

	offset := 0
	for _, line := range strings.SplitAfter(dql, "\n") {
		start := offset
		end := start + len(line)
		if start >= len(inBlock) || inBlock[start] {
			offset = end
			continue
		}
		spec, alias, ok := parseLegacyImportLine(line)
		if !ok {
			offset = end
			continue
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
			offset = end
			continue
		}
		imports = append(imports, aImport)
		ranges = append(ranges, legacyImportRange{start: start, end: end})
		offset = end
	}

	return uniqueTypeImports(imports), ranges, diags
}

func findLegacyImportBlocks(dql string) []legacyImportBlockSpec {
	var result []legacyImportBlockSpec
	for lineStart := 0; lineStart < len(dql); {
		lineEnd := lineStart
		for lineEnd < len(dql) && dql[lineEnd] != '\n' {
			lineEnd++
		}

		pos := skipInlineSpaces(dql, lineStart)
		if hasWordFoldAt(dql, pos, "import") {
			pos = skipSpaces(dql, pos+len("import"))
			if pos < len(dql) && dql[pos] == '(' {
				body, end, ok := readCallBody(dql, pos)
				if ok {
					result = append(result, legacyImportBlockSpec{
						start:     lineStart,
						end:       end + 1,
						bodyStart: pos + 1,
						bodyEnd:   pos + 1 + len(body),
					})
					lineStart = end + 1
					continue
				}
			}
		}

		if lineEnd < len(dql) {
			lineStart = lineEnd + 1
		} else {
			break
		}
	}
	return result
}

type legacyImportItem struct {
	spec   string
	alias  string
	offset int
}

func parseLegacyImportItems(input string, base int) []legacyImportItem {
	var result []legacyImportItem
	for i := 0; i < len(input); {
		i = skipLegacyImportSeparators(input, i)
		if i >= len(input) {
			break
		}
		start := i
		spec, end, ok := readQuotedAt(input, i)
		if !ok {
			i++
			continue
		}
		i = skipSpaces(input, end)
		alias := ""
		if hasWordFoldAt(input, i, "alias") {
			i = skipSpaces(input, i+len("alias"))
			aliasValue, aliasEnd, ok := readQuotedAt(input, i)
			if !ok {
				i = end
				continue
			}
			alias = aliasValue
			i = aliasEnd
		}
		result = append(result, legacyImportItem{
			spec:   strings.TrimSpace(spec),
			alias:  strings.TrimSpace(alias),
			offset: base + start,
		})
	}
	return result
}

func parseLegacyImportLine(line string) (spec, alias string, ok bool) {
	input := strings.TrimSpace(line)
	if input == "" || !hasWordFoldAt(input, 0, "import") {
		return "", "", false
	}
	index := skipSpaces(input, len("import"))
	specValue, end, ok := readQuotedAt(input, index)
	if !ok {
		return "", "", false
	}
	index = skipSpaces(input, end)
	aliasValue := ""
	if hasWordFoldAt(input, index, "alias") {
		index = skipSpaces(input, index+len("alias"))
		value, aliasEnd, ok := readQuotedAt(input, index)
		if !ok {
			return "", "", false
		}
		aliasValue = value
		index = skipSpaces(input, aliasEnd)
	}
	if index != len(input) {
		return "", "", false
	}
	return strings.TrimSpace(specValue), strings.TrimSpace(aliasValue), true
}

func readQuotedAt(input string, index int) (string, int, bool) {
	if index < 0 || index >= len(input) {
		return "", index, false
	}
	quote := input[index]
	if quote != '\'' && quote != '"' {
		return "", index, false
	}
	for i := index + 1; i < len(input); i++ {
		if input[i] == '\\' && i+1 < len(input) {
			i++
			continue
		}
		if input[i] == quote {
			return input[index+1 : i], i + 1, true
		}
	}
	return "", index, false
}

func skipLegacyImportSeparators(input string, index int) int {
	for index < len(input) {
		switch input[index] {
		case ' ', '\t', '\n', '\r', ',', ';':
			index++
		default:
			return index
		}
	}
	return index
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
