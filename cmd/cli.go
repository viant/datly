package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/viant/afs"
	"github.com/viant/afs/cache"
	"github.com/viant/afs/file"
	"github.com/viant/afs/matcher"
	"github.com/viant/afs/modifier"
	soption "github.com/viant/afs/option"
	"github.com/viant/datly/auth/jwt"
	"github.com/viant/datly/cmd/command"
	soptions "github.com/viant/datly/cmd/options"
	"github.com/viant/datly/gateway"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/openapi3"
	"gopkg.in/yaml.v3"
	"io"
	"os"
	"path"
	"strings"
)

func (s *Builder) build() (*standalone.Server, error) {
	if s.options.IsPluginBuildMode() {
		return nil, s.buildBinary(s.options.PluginSrc[0], s.options.PluginDst, s.options.PluginName, path.Join(pluginDirectory, pluginFile), "", true)
	}

	if s.options.IsExecBuildMode() {
		return nil, s.buildBinary(s.options.ModuleSrc[0], s.options.ModuleDst, s.options.ModuleName, s.options.ModuleMain, "exec", false)
	}

	_, _ = s.logger.Write([]byte(reportContent("------------ config ------------\n\t "+s.options.ConfigURL, s.options.ConfigURL)))

	authenticator, err := jwt.Init(s.config.Config, nil)
	if authenticator != nil {
		fmt.Printf("with auth Service: %T\n", authenticator)
	}

	s.flushLogs(s.logger)

	dumped := false
	if s.options.PrepareRule != "" {
		dumpConfiguration("", s.options.DSQLOutput, s.options)
		dumped = true
	}

	if s.options.WriteLocation != "" {
		dumpConfiguration(s.options.WriteLocation, s.options.RoutePrefix, s.options)
		if !s.options.hasPort {
			return nil, nil
		}
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
		fmt.Printf("[INFO] starting cache warmup for: %v\n", s.options.WarmupURIs)
		response := warmup.PreCache(srv.Service.PreCachables, s.options.WarmupURIs...)
		data, _ := json.Marshal(response)
		fmt.Printf("%s\n", data)
	}

	if err != nil {
		return nil, err
	}
	if s.options.OpenApiURL != "" {
		//TODO: add opeanpi3.Spec to Config
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
	//var err error
	builder := &Builder{
		options:    options,
		tablesMeta: NewTableMetaRegistry(),
		logger:     logger,
		fs:         afs.New(),
		fileNames:  newUniqueIndex(false),
		viewNames:  newUniqueIndex(true),
		types:      newUniqueIndex(true),
		bundles:    map[string]*bundleMetadata{},
	}

	return builder, builder.Build(context.TODO())
}

func New(version string, args soptions.Arguments, logger io.Writer) (*standalone.Server, error) {

	tryNewVersion := true
	if tryNewVersion {
		os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
		options, err := buildOptions(args)
		if err != nil {
			return nil, err
		}
		if err := options.Init(context.Background()); err != nil {
			return nil, err
		}
		cmd := command.New()
		done, err := cmd.Exec(context.Background(), options)
		if err != nil || done {
			return nil, err
		}

	}

	opts := &Options{}
	if _, err := flags.ParseArgs(opts, args); err != nil {
		return nil, err
	}
	if opts.Version {
		fmt.Printf("Datly: version: %v\n", version)
		return nil, nil
	}
	if isOption("-h", args) {
		return nil, nil
	}
	return runInLegacyMode(opts, logger)
}

func buildOptions(args soptions.Arguments) (*soptions.Options, error) {
	var opts *soptions.Options
	if (args.SubMode() || args.IsHelp()) && !args.IsLegacy() {
		opts = soptions.NewOptions(args)
		if _, err := flags.ParseArgs(opts, args); err != nil {
			return nil, err
		}
		if args.IsHelp() {
			return nil, nil
		}

	} else {
		options := &Options{}
		if _, err := flags.ParseArgs(options, args); err != nil {
			return nil, err
		}
		opts = options.BuildOption()
	}
	return opts, nil
}

var fs = afs.New()

func runInLegacyMode(options *Options, logger io.Writer) (*standalone.Server, error) {
	var err error
	if options.Package.RuleSourceURL != "" {
		return nil, packageConfig(options)
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
	if builder == nil {
		return nil, nil
	}
	return builder.build()
}

func packageConfig(options *Options) error {
	if options.Package.RuleDestURL == "" {
		return fmt.Errorf("package rule dest url was empty")
	}
	ruleSourceURL := normalizeURL(options.Package.RuleSourceURL)
	ruleDestURL := normalizeURL(options.Package.RuleDestURL)
	cacheSetting := soption.WithCache(gateway.PackageFile, "gzip")
	return cache.Package(context.Background(), ruleSourceURL, ruleDestURL,
		cacheSetting,
		matcher.WithExtExclusion(".so", "so", ".gz", "gz"),
	)
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

func reportContent(message string, URL string) string {
	fs := afs.New()
	data, _ := fs.DownloadWithURL(context.Background(), URL)
	return fmt.Sprintf("%v %s\n", message, data)
}

func isOption(key string, args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
