package cmd

import (
	"context"
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/modifier"
	"github.com/viant/datly/auth/cognito"
	"github.com/viant/datly/gateway/runtime/lambda"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/openapi3"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

func New(args []string, logger io.Writer) (*standalone.Server, error) {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	options := &Options{}
	_, err := flags.ParseArgs(options, args)
	if isHelOption(args) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	options.Init()
	ctx := context.Background()
	config, err := loadConfig(ctx, options)
	if err != nil {
		return nil, err
	}
	err = initConfig(config, options)
	if err != nil {
		return nil, err
	}
	reportContent(logger, "------------ config ------------\n\t "+options.ConfigURL, options.ConfigURL)
	var authService *cognito.Service
	if config.Cognito != nil {
		if authService, err = lambda.InitAuthService(config.Config); err != nil {
			return nil, err
		}
		fmt.Printf("with auth Service: %T\n", authService)
	}

	if URL := options.DepURL("connections"); URL != "" {
		reportContent(logger, "---------- connections: -----------\n\t"+URL, URL)
	}

	if URL := options.RouterURL(); URL != "" {
		reportContent(logger, "-------------- view --- -----------\n\t"+URL, URL)
	}
	if options.WriteLocation != "" {
		dumpConfiguration(options)
		return nil, nil
	}

	var srv *standalone.Server
	if authService == nil {
		srv, err = standalone.New(config)
	} else {
		srv, err = standalone.NewWithAuth(config, authService)
	}

	if err != nil {
		return nil, err
	}

	if options.OpenApiURL != "" {
		//TODO: add opeanpi3.Info to Config
		openapiSpec, _ := router.GenerateOpenAPI3Spec(openapi3.Info{}, srv.Routes()...)
		openApiMarshal, _ := yaml.Marshal(openapiSpec)
		_ = os.WriteFile(options.OpenApiURL, openApiMarshal, file.DefaultFileOsMode)
	}

	if err != nil {
		return nil, err
	}

	_, _ = logger.Write([]byte(fmt.Sprintf("starting endpoint: %v\n", config.Endpoint.Port)))
	return srv, nil
}

func dumpConfiguration(options *Options) {
	fs := afs.New()
	destURL := normalizeURL(options.WriteLocation)
	os.MkdirAll(destURL, file.DefaultDirOsMode)
	srcURL := "mem://localhost/dev"
	fs.Copy(context.Background(), "mem://localhost/dev", destURL, modifier.Replace(map[string]string{
		srcURL: destURL,
	}))
}

func reportContent(logger io.Writer, message string, URL string) {
	_, _ = logger.Write([]byte(message))
	fs := afs.New()
	data, _ := fs.DownloadWithURL(context.Background(), URL)
	_, _ = logger.Write([]byte(fmt.Sprintf("%s\n", data)))
}

func isHelOption(args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
