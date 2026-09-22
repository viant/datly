package command

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	shapevalidate "github.com/viant/datly/repository/shape/validate"
)

func (s *Service) Validate(ctx context.Context, opts *options.Options) error {
	validate := opts.Validate
	if validate == nil {
		return fmt.Errorf("validate options not set")
	}
	compiler := shapeCompile.New()
	loader := shapeLoad.New()
	var validated []string
	for _, sourceURL := range validate.Source {
		dql, err := s.readSource(ctx, sourceURL)
		if err != nil {
			return fmt.Errorf("failed to read %s: %w", sourceURL, err)
		}
		shapeSource := &shape.Source{
			Name:      strings.TrimSuffix(filepath.Base(url.Path(sourceURL)), filepath.Ext(sourceURL)),
			Path:      url.Path(sourceURL),
			DQL:       strings.TrimSpace(dql),
			Connector: validateDefaultConnectorName(validate),
		}
		planResult, err := compiler.Compile(ctx, shapeSource, validateCompileOptions(validate)...)
		if err != nil {
			return fmt.Errorf("validate %s: %w", sourceURL, err)
		}
		if err = validateDiagnostics(sourceURL, planResult); err != nil {
			return err
		}
		if err = validatePlannedSQLAssets(ctx, s, shapeSource, planResult); err != nil {
			return fmt.Errorf("validate %s: %w", sourceURL, err)
		}
		resourceArtifacts, err := loader.LoadResource(ctx, planResult, shape.WithLoadTypeContextPackages(true))
		if err != nil {
			return fmt.Errorf("validate %s: %w", sourceURL, err)
		}
		if err = shapevalidate.ValidateRelations(resourceArtifacts.Resource); err != nil {
			return fmt.Errorf("validate %s: %w", sourceURL, err)
		}
		validated = append(validated, filepath.Clean(url.Path(sourceURL)))
	}
	sort.Strings(validated)
	for _, item := range validated {
		fmt.Printf("validated %s\n", item)
	}
	return nil
}

func validateDefaultConnectorName(v *options.Validate) string {
	if v == nil || len(v.Connectors) == 0 {
		return ""
	}
	parts := strings.SplitN(v.Connectors[0], "|", 2)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func validateCompileOptions(v *options.Validate) []shape.CompileOption {
	var opts []shape.CompileOption
	if v != nil && v.Strict {
		opts = append(opts, shape.WithCompileStrict(true))
	}
	opts = append(opts, shape.WithLinkedTypes(false))
	return opts
}

func validateDiagnostics(sourceURL string, result *shape.PlanResult) error {
	planned, ok := plan.ResultFrom(result)
	if !ok || planned == nil {
		return nil
	}
	var issues []string
	for _, diag := range planned.Diagnostics {
		if diag == nil || diag.Severity != "error" {
			continue
		}
		issues = append(issues, diag.Error())
	}
	if len(issues) == 0 {
		return nil
	}
	return fmt.Errorf("validate %s: %s", sourceURL, strings.Join(issues, "; "))
}

func validatePlannedSQLAssets(ctx context.Context, s *Service, source *shape.Source, result *shape.PlanResult) error {
	planned, ok := plan.ResultFrom(result)
	if !ok || planned == nil {
		return nil
	}
	assets := collectPlannedSQLAssets(source, planned)
	for _, asset := range assets {
		if _, err := s.fs.DownloadWithURL(ctx, asset); err != nil {
			return fmt.Errorf("missing SQL asset %s: %w", asset, err)
		}
	}
	return nil
}

func collectPlannedSQLAssets(source *shape.Source, planned *plan.Result) []string {
	seen := map[string]bool{}
	var result []string
	appendAsset := func(candidate string) {
		raw := strings.TrimSpace(candidate)
		if !isExplicitSourceAsset(source, raw) {
			return
		}
		candidate = raw
		if candidate == "" {
			return
		}
		if strings.Contains(candidate, "://") {
			candidate = url.Path(candidate)
		}
		if !filepath.IsAbs(candidate) {
			baseDir := ""
			if source != nil {
				baseDir = source.BaseDir()
			}
			if baseDir != "" {
				candidate = filepath.Join(baseDir, filepath.FromSlash(candidate))
			}
		}
		candidate = filepath.Clean(candidate)
		if candidate == "." || seen[candidate] {
			return
		}
		seen[candidate] = true
		result = append(result, file.Scheme+"://"+filepath.ToSlash(candidate))
	}
	for _, route := range planned.Components {
		if route == nil {
			continue
		}
		appendAsset(route.SourceURL)
		appendAsset(route.SummaryURL)
	}
	for _, item := range planned.Views {
		if item == nil {
			continue
		}
		appendAsset(item.SQLURI)
		appendAsset(item.SummaryURL)
	}
	sort.Strings(result)
	return result
}

func isExplicitSourceAsset(source *shape.Source, candidate string) bool {
	candidate = strings.TrimSpace(candidate)
	if candidate == "" {
		return false
	}
	if source == nil || strings.TrimSpace(source.DQL) == "" {
		return true
	}
	clean := filepath.ToSlash(candidate)
	if strings.Contains(source.DQL, clean) {
		return true
	}
	base := path.Base(clean)
	return base != "" && strings.Contains(source.DQL, base)
}
