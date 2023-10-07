package translator

import (
	"context"
	"fmt"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/asset"
	dpath "github.com/viant/datly/repository/path"
	"path"
)

func (s *Service) buildStaticContent(ctx context.Context, rule *options.Rule, resource *Resource, opts *options.Options) error {
	contentSourceURL := resource.Rule.ContentURL
	baseRuleURL := s.Repository.RuleBaseURL(resource.rule)
	contentBaseURL := s.Repository.ContentBaseURL(resource.rule)
	aPath := &dpath.Path{}
	aPath.Cors = resource.Rule.Cors
	aPath.APIKey = resource.Rule.APIKey
	ruleName := s.Repository.RuleName(resource.rule)
	aPath.Path = resource.Rule.Path
	aPath.ContentURL = path.Join(resource.rule.ModulePrefix, ruleName)
	items := &dpath.Item{Paths: []*dpath.Path{aPath}}
	data, err := asset.EncodeYAML(items)
	if err != nil {
		return fmt.Errorf("failed to encode: %+v, %w", items, err)
	}
	ruleSource := string(data)
	if err = s.fs.Copy(ctx, contentSourceURL, url.Join(contentBaseURL, ruleName)); err != nil {
		return err
	}
	s.Repository.PersistAssets = true
	s.Repository.Files.Append(asset.NewFile(url.Join(baseRuleURL, ruleName+".yaml"), ruleSource))
	return nil
}
