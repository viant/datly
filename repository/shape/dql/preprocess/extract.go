package preprocess

import (
	"strings"

	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

func extractSQLAndContext(dql string) (string, *typectx.Context, *dqlshape.Directives, []*dqlshape.Diagnostic) {
	ctx := &typectx.Context{}
	directives := &dqlshape.Directives{}
	if dql == "" {
		return "", nil, nil, nil
	}
	mask := make([]bool, len(dql))
	var diagnostics []*dqlshape.Diagnostic

	blocks := extractSetDirectiveBlocks(dql)
	for _, block := range blocks {
		applyMask(mask, dql, block.start, block.end)
		if block.kind != directiveSettings {
			continue
		}
		diagnostics = append(diagnostics, parseSettingsDirectives(block.body, dql, block.start, directives)...)
	}

	lines := strings.SplitAfter(dql, "\n")
	if len(lines) == 0 {
		lines = []string{dql}
	}

	offset := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		lineStart := offset
		lineEnd := offset + len(line)
		if isTypeContextDirectiveLine(trimmed) {
			diagnostics = append(diagnostics, parseTypeContextDirective(trimmed, dql, offsetOfFirstNonSpace(line, offset), ctx)...)
			applyMask(mask, dql, lineStart, lineEnd)
			offset += len(line)
			continue
		}
		if kind := lineDirectiveKind(trimmed); kind != directiveUnknown {
			if !hasMasked(mask, lineStart, lineEnd) {
				if kind != directiveSettings {
					applyMask(mask, dql, lineStart, lineEnd)
					offset += len(line)
					continue
				}
				diagnostics = append(diagnostics, parseSettingsDirectives(trimmed, dql, offsetOfFirstNonSpace(line, offset), directives)...)
				applyMask(mask, dql, lineStart, lineEnd)
			}
			offset += len(line)
			continue
		}
		if isDirectiveLine(trimmed) {
			applyMask(mask, dql, lineStart, lineEnd)
		}
		offset += len(line)
	}
	masked := []byte(dql)
	for i := 0; i < len(masked); i++ {
		if !mask[i] {
			continue
		}
		if masked[i] == '\n' || masked[i] == '\r' {
			continue
		}
		masked[i] = ' '
	}
	return string(masked), ctx, directives, diagnostics
}

func applyMask(mask []bool, text string, start, end int) {
	if start < 0 {
		start = 0
	}
	if end > len(text) {
		end = len(text)
	}
	if end <= start {
		return
	}
	for i := start; i < end; i++ {
		if text[i] == '\n' || text[i] == '\r' {
			continue
		}
		mask[i] = true
	}
}

func hasMasked(mask []bool, start, end int) bool {
	if start < 0 {
		start = 0
	}
	if end > len(mask) {
		end = len(mask)
	}
	if end <= start {
		return false
	}
	for i := start; i < end; i++ {
		if mask[i] {
			return true
		}
	}
	return false
}

func offsetOfFirstNonSpace(line string, base int) int {
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return base + i
		}
	}
	return base
}
