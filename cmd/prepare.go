package cmd

import (
	"context"
	"encoding/json"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/option"
	codegen "github.com/viant/datly/internal/codegen"
	"github.com/viant/datly/internal/translator"
	"github.com/viant/datly/router"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/template/sanitize"
	"path"
	"strings"
)

func (s *Builder) extractRouteSettings(sourceSQL []byte) (string, string) {
	hint := sanitize.ExtractHint(string(sourceSQL))
	SQL := strings.Replace(string(sourceSQL), hint, "", 1)
	return hint, SQL
}

func (s *Builder) buildCodeTemplate(ctx context.Context, builder *routeBuilder, sourceSQL []byte, httpMethod string) (*codegen.Template, error) {
	SQL, routeConfig, err := s.loadRouteConfig(sourceSQL, httpMethod)
	if err != nil {
		return nil, err
	}
	paramIndex := NewParametersIndex(routeConfig, map[string]*sanitize.ParameterHint{})
	configurer, err := NewConfigProviderReader("", SQL, routeConfig, router.ServiceTypeReader, paramIndex, &s.options.Prepare, &s.options.Connector, builder)
	if err != nil {
		return nil, err
	}
	aConfig := configurer.ViewConfig()
	connectorRef, err := s.ConnectorRef(shared.FirstNotEmpty(aConfig.expandedTable.Connector, s.options.Connector.DbName))
	if err != nil {
		return nil, err
	}

	db, err := s.DB(connectorRef)
	if err != nil {
		return nil, err
	}

	if pkg := s.options.GoModulePkg; pkg == "" {
		var parent string
		destPath := url.Path(s.Options.Generate.Dest)
		parent, s.options.GoModulePkg = path.Split(destPath)
		if s.options.RelativePath == "" {
			s.options.RelativePath = parent
		}
	}

	if err = aConfig.buildSpec(ctx, db, s.options.GoModulePkg); err != nil {
		return nil, err
	}

	rule := &translator.Rule{}
	data, _ := json.Marshal(routeConfig)
	_ = json.Unmarshal(data, rule)
	template := codegen.NewTemplate(rule, aConfig.Spec)
	template.BuildTypeDef(aConfig.Spec, aConfig.outputConfig.GetField())
	var opts = []codegen.Option{codegen.WithHTTPMethod(httpMethod), codegen.WithLang(s.Options.Generate.Lang)}
	template.BuildState(aConfig.Spec, aConfig.outputConfig.GetField(), opts...)
	template.BuildLogic(aConfig.Spec, opts...)
	return template, nil
}

func (s *Builder) loadRouteConfig(sourceSQL []byte, httpMethod string) (string, *option.RouteConfig, error) {
	hint, SQL := s.extractRouteSettings(sourceSQL)
	routeConfig := &option.RouteConfig{Method: httpMethod}
	if err := tryUnmarshalHint(hint, routeConfig); err != nil {
		return "", nil, err
	}
	return SQL, routeConfig, nil
}
