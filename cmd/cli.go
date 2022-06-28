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
	"log"
	"os"
)

func Run(args []string) {
	os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	options := &Options{}
	_, err := flags.ParseArgs(options, args)
	if isHelOption(args) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	options.Init()
	ctx := context.Background()
	config, err := loadConfig(ctx, options)
	if err != nil {
		log.Fatal(err)
	}
	err = initConfig(config, options)
	if err != nil {
		log.Fatal(err)
	}
	reportContent("------------ config ------------\n\t "+options.ConfigURL, options.ConfigURL)
	var authService *cognito.Service
	if config.Cognito != nil {
		if authService, err = lambda.InitAuthService(config.Config); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("with auth Service: %T\n", authService)
	}

	if URL := options.DepURL("connections"); URL != "" {
		reportContent("---------- connections: -----------\n\t"+URL, URL)
	}

	if URL := options.RouterURL(); URL != "" {
		reportContent("-------------- view --- -----------\n\t"+URL, URL)
	}
	if options.WriteLocation != "" {
		dumpConfiguration(options)
		return
	}

	var srv *standalone.Server
	if authService == nil {
		srv, err = standalone.New(config)
	} else {
		srv, err = standalone.NewWithAuth(config, authService)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("starting endpoint: %v\n", config.Endpoint.Port)
	_ = srv.ListenAndServe()
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

func reportContent(message string, URL string) {
	fmt.Println(message)
	fs := afs.New()
	data, _ := fs.DownloadWithURL(context.Background(), URL)
	fmt.Printf("%s\n", data)
}

func isHelOption(args []string) bool {
	for _, arg := range args {
		if arg == "-h" {
			return true
		}
	}
	return false
}
