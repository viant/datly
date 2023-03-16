package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/modifier"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/openapi3"
	"github.com/viant/datly/view"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path"
	"strings"
)

func (s *Builder) build() (*standalone.Server, error) {
	if s.options.IsPluginBuildMode() {
		return nil, s.buildBinary(s.options.PluginSrc, s.options.PluginDst, s.options.PluginName, path.Join(pluginDirectory, pluginFile), "")
	}

	if s.options.IsExecBuildMode() {
		return nil, s.buildBinary(s.options.ModuleSrc, s.options.ModuleDst, "", s.options.ModuleMain, "exec")
	}

	reportContent(s.logger, "------------ config ------------\n\t "+s.options.ConfigURL, s.options.ConfigURL)

	authenticator, err := jwt.Init(s.config.Config, nil)
	if authenticator != nil {
		fmt.Printf("with auth Service: %T\n", authenticator)
	}

	if URL := s.options.DepURL("connections"); URL != "" {
		reportContent(s.logger, "---------- connections: -----------\n\t"+URL, URL)
	}

	if URL := s.options.RouterURL(); URL != "" {
		reportContent(s.logger, "-------------- view --- -----------\n\t"+URL, URL)
	}

	dumped := false
	if s.options.PrepareRule != "" {
		dumpConfiguration("", s.options.DSQLOutput, s.options)
		dumped = true
	}

	if s.options.WriteLocation != "" {
		dumpConfiguration(s.options.WriteLocation, s.options.RoutePrefix, s.options)
		return nil, nil
	}

	if dumped {
		return nil, nil
	}

	var srv *standalone.Server
	if authenticator == nil {
		srv, err = standalone.New(s.config)
	} else {
		srv, err = standalone.NewWithAuth(s.config, authenticator)
	}

	if len(s.options.WarmupURIs) > 0 {
		fmt.Printf("starting cache warmup for: %v\n", s.options.WarmupURIs)
		response := warmup.PreCache(srv.Service.PreCachables, s.options.WarmupURIs...)
		data, _ := json.Marshal(response)
		fmt.Printf("%s\n", data)
	}

	if err != nil {
		return nil, err
	}
	if s.options.OpenApiURL != "" {
		//TODO: add opeanpi3.Info to Config
		openapiSpec, _ := router.GenerateOpenAPI3Spec(openapi3.Info{}, srv.Routes()...)
		openApiMarshal, _ := yaml.Marshal(openapiSpec)
		_ = os.WriteFile(s.options.OpenApiURL, openApiMarshal, file.DefaultFileOsMode)
	}

	if err != nil {
		return nil, err
	}

	_, _ = s.logger.Write([]byte(fmt.Sprintf("starting endpoint: %v\n", s.config.Endpoint.Port)))
	return srv, nil
}

func normalizeMetaTemplateSQL(SQL string, holderViewName string) string {
	return strings.Replace(SQL, "$View."+holderViewName+".SQL", "$View.NonWindowSQL", 1)
}

func NewBuilder(options *Options, logger io.Writer) (*Builder, error) {
	builder := &Builder{
		options:    options,
		tablesMeta: NewTableMetaRegistry(),
		logger:     logger,
		fs:         afs.New(),
		routeBuilder: &routeBuilder{
			views: map[string]*view.View{},
			routerResource: &router.Resource{
				Resource: view.EmptyResource(),
			},
			paramsIndex: NewParametersIndex(nil, nil),
			option: &option.RouteConfig{
				Declare: map[string]string{},
				Const:   map[string]interface{}{},
			},
		},
		fileNames: newUniqueIndex(false),
		viewNames: newUniqueIndex(true),
		types:     newUniqueIndex(true),
		bundles:   map[string]string{},
	}

	return builder, builder.Build(context.TODO())
}

func New(version string, args []string, logger io.Writer) (*standalone.Server, error) {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	options := &Options{}
	_, err := flags.ParseArgs(options, args)

	if options.Version {
		fmt.Printf("Datly: version: %v\n", version)
		return nil, nil
	}

	if isOption("-h", args) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	if err = options.Init(); err != nil {
		return nil, err
	}

	builder, err := NewBuilder(options, logger)
	if err != nil {
		return nil, err
	}

	return builder.build()
}

func dumpConfiguration(location, folder string, options *Options) {
	dumpFolder(options, location, folder)
}

func dumpFolder(options *Options, location, folder string) {
	fs := afs.New()
	destURL := normalizeURL(options.WriteLocation)
	os.MkdirAll(destURL, file.DefaultDirOsMode)
	srcURL := fmt.Sprintf("mem://localhost/%v", folder)
	fs.Copy(context.Background(), srcURL, destURL, modifier.Replace(map[string]string{
		srcURL: destURL,
	}))
}

func reportContent(logger io.Writer, message string, URL string) {
	_, _ = logger.Write([]byte(message))
	fs := afs.New()
	data, _ := fs.DownloadWithURL(context.Background(), URL)
	_, _ = logger.Write([]byte(fmt.Sprintf("%s\n", data)))
}

func isOption(key string, args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
