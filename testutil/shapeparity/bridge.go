package shapeparity

import (
	"context"
	"fmt"
	"strings"

	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/datly/repository/shape/dql/sanitize"
	dqlscan "github.com/viant/datly/repository/shape/dql/scan"
)

// ScanDQL translates a DQL file through the legacy internal/translator pipeline
// and returns a scan.Result. This bridges internal/translator for parity tests
// without requiring repository/shape to depend on internal/*.
func ScanDQL(ctx context.Context, req *dqlscan.Request) (*dqlscan.Result, error) {
	if req == nil || req.DQLURL == "" {
		return nil, fmt.Errorf("dql scan: DQLURL was empty")
	}
	fs := afs.New()
	sourceURL := req.DQLURL
	project := inferProject(req.DQLURL)
	translate := &options.Translate{}
	translate.Rule.Project = project
	translate.Rule.Source = []string{sourceURL}
	translate.Rule.ModulePrefix = req.ModulePrefix
	translate.Repository.RepositoryURL = req.Repository
	translate.Repository.APIPrefix = req.APIPrefix
	if len(req.Connectors) > 0 {
		translate.Repository.Connectors = append(translate.Repository.Connectors, req.Connectors...)
	}
	if req.ConfigURL != "" {
		translate.Repository.Configs.Append(req.ConfigURL)
	}
	if initErr := translate.Init(ctx); initErr != nil {
		return nil, initErr
	}
	if req.ConfigURL == "" {
		translate.Repository.Configs = nil
	}
	if translate.Rule.ModulePrefix == "" {
		translate.Rule.ModulePrefix = "platform"
	}

	svc := translator.New(translator.NewConfig(&translate.Repository), fs)
	if initErr := svc.Init(ctx); initErr != nil {
		return nil, initErr
	}
	if initErr := svc.InitSignature(ctx, &translate.Rule); initErr != nil {
		return nil, initErr
	}
	dsql, loadErr := translate.Rule.LoadSource(ctx, fs, translate.Rule.SourceURL())
	if loadErr != nil {
		return nil, loadErr
	}
	translate.Rule.NormalizeComponent(&dsql)
	dsql = sanitize.SQL(dsql, sanitize.Options{Declared: sanitize.Declared(dsql)})
	top := &options.Options{Translate: translate}
	if initErr := svc.Translate(ctx, &translate.Rule, dsql, top); initErr != nil {
		return nil, initErr
	}
	ruleName := svc.Repository.RuleName(&translate.Rule)
	targetSuffix := "/" + ruleName + ".yaml"

	scanner := dqlscan.New()
	for _, item := range svc.Repository.Files {
		if !strings.HasSuffix(item.URL, targetSuffix) {
			continue
		}
		if strings.Contains(item.URL, "/.meta/") {
			continue
		}
		return scanner.Result(ruleName, []byte(item.Content), dsql, req)
	}
	for _, item := range svc.Repository.Files {
		if strings.HasSuffix(item.URL, targetSuffix) {
			return scanner.Result(ruleName, []byte(item.Content), dsql, req)
		}
	}
	return nil, fmt.Errorf("dql scan: generated YAML not found for %s", ruleName)
}

func inferProject(dqlURL string) string {
	base, _ := url.Split(dqlURL, file.Scheme)
	if idx := strings.Index(base, "/dql/"); idx != -1 {
		return base[:idx]
	}
	return base
}
