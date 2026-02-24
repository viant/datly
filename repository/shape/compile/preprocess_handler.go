package compile

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/compile/pipeline"
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	dqlstmt "github.com/viant/datly/repository/shape/dql/statement"
)

type handlerPreprocessResult struct {
	Pre             *dqlpre.Result
	Statements      dqlstmt.Statements
	Decision        pipeline.Decision
	EffectiveSource *shape.Source
}

func buildHandlerIfNeeded(source *shape.Source, pre *dqlpre.Result, statements dqlstmt.Statements, decision pipeline.Decision, layout compilePathLayout) *handlerPreprocessResult {
	ret := &handlerPreprocessResult{
		Pre:             pre,
		Statements:      statements,
		Decision:        decision,
		EffectiveSource: source,
	}
	if source == nil {
		return ret
	}
	unknownOnly := decision.HasUnknown && !decision.HasRead && !decision.HasExec
	if !unknownOnly && !isHandlerSignal(source) {
		return ret
	}
	if buildHandlerFromContractIfNeeded(ret, source, layout) {
		return ret
	}
	if buildGeneratedFallbackIfNeeded(ret, source, layout) {
		return ret
	}
	return ret
}

func buildHandlerFromContractIfNeeded(ret *handlerPreprocessResult, source *shape.Source, layout compilePathLayout) bool {
	_ = ret
	_ = source
	_ = layout
	return false
}

func buildGeneratedFallbackIfNeeded(ret *handlerPreprocessResult, source *shape.Source, layout compilePathLayout) bool {
	if ret == nil || source == nil {
		return false
	}
	_ = layout
	generated := strings.TrimSpace(resolveGeneratedCompanionDQL(source))
	if generated == "" {
		return false
	}
	candidate := dqlpre.Prepare(generated)
	if strings.TrimSpace(candidate.SQL) == "" {
		return false
	}
	candidateStatements := dqlstmt.New(candidate.SQL)
	candidateDecision := pipeline.Classify(candidateStatements)
	if !candidateDecision.HasRead && !candidateDecision.HasExec {
		return false
	}
	ret.Pre = candidate
	ret.Statements = candidateStatements
	ret.Decision = candidateDecision
	return true
}

func resolveGeneratedLegacySource(source *shape.Source) *shape.Source {
	if source == nil || strings.TrimSpace(source.Path) == "" {
		return nil
	}
	path := filepath.Clean(source.Path)
	normalized := filepath.ToSlash(path)
	genIdx := strings.Index(normalized, "/gen/")
	if genIdx == -1 {
		return nil
	}
	prefix := normalized[:genIdx]
	suffix := strings.TrimPrefix(normalized[genIdx+len("/gen/"):], "/")
	parts := strings.Split(suffix, "/")
	if len(parts) < 2 {
		return nil
	}
	fileName := parts[len(parts)-1]
	stem := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	candidates := []string{
		filepath.FromSlash(prefix + "/" + fileName),
		filepath.FromSlash(prefix + "/" + stem + ".sql"),
		filepath.FromSlash(prefix + "/" + stem + ".dql"),
	}
	for _, candidate := range candidates {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		clone := *source
		clone.Path = candidate
		clone.DQL = string(data)
		return &clone
	}
	return nil
}

func isHandlerSignal(source *shape.Source) bool {
	if source == nil {
		return false
	}
	settings := extractRuleSettings(source, nil)
	if settings != nil {
		if strings.TrimSpace(settings.Type) != "" {
			return true
		}
		if method := strings.TrimSpace(strings.ToUpper(settings.Method)); method != "" && method != "GET" {
			return true
		}
		if strings.Contains(strings.ToLower(strings.TrimSpace(settings.URI)), "/proxy") {
			return true
		}
	}
	raw := strings.ToLower(strings.TrimSpace(source.DQL))
	return strings.Contains(raw, "$nop(") || strings.Contains(raw, "$proxy(")
}
