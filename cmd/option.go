package cmd

import (
	"context"
	_ "embed"
	"github.com/viant/afs"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/view"
)

type (
	//deprecated
	Options struct {
		Port               int `short:"p" long:"port" description:"port"  `
		hasPort            bool
		RouteURL           string `short:"r" long:"routeURL" description:"route URL"  `
		DependencyURL      string `short:"d" long:"deps" description:"dependencies URL" `
		ConfigURL          string `short:"c" long:"config" description:"configuration URL" `
		PartialConfigURL   string `short:"e" long:"partialConfig" description:"partial configuration file URL"`
		isInit             bool
		JWTVerifierRSAKey  string `short:"j" long:"jwtRSAKey" description:"PublicKeyPath|EncKey" `
		JWTVerifierHMACKey string `short:"m" long:"jwtHMACKey" description:"digest key" `
		WriteLocation      string `short:"w" long:"write" description:"dump all config files to specified location" `
		BuildMode          string `long:"buildMode" description:"values: plugin - generates only plugins, pluginless - generates rule without plugins, plugins need to be created later"`
		Generate
		Connector
		CacheWarmup
		Prepare
		OpenApiURL   string `short:"o" long:"openapi"`
		Version      bool   `short:"v" long:"version"  description:"build version"`
		RelativePath string `long:"relative" description:"allow to control relative path where path is used"`
		RoutePrefix  string `short:"x" long:"routePrefix" description:"route prefix default dev"`
		ApiURIPrefix string `short:"i" long:"apiPrefix" description:"api prefix default /v1/api/"`
		Plugins
		Package
		Module
		AssetsURL     string `short:"a" long:"assetsURL" description:"assets destination"`
		ConstURL      string `long:"constURL" description:"path where const files are stored"`
		Legacy        bool   `short:"l"`
		cache         *view.Cache
		SubstituesURL []string `long:"substituesURL" description:"substitues URL, expands template before processing"`
		JobURL        string   `short:"z" long:"joburl" description:"job url"`
	}

	Package struct {
		RuleSourceURL string `short:"P" long:"packageSrc" description:"package rule source URL " `
		RuleDestURL   string `short:"R" long:"packageDest" description:"package rule destination URL rewrite" `
	}

	CacheWarmup struct {
		WarmupURIs []string `short:"u" long:"wuri" description:"uri to warmup cache" `
	}

	Connector struct {
		connectors []*view.Connector
		Connects   []string `short:"C" long:"conn" description:"name|driver|dsn|secret|secretkey" `
		DbName     string   `short:"V" long:"dbname" description:"db/connector name" `
		Driver     string   `short:"D" long:"driver" description:"driver" `
		DSN        string   `short:"A" long:"dsn" description:"DSN" `
		Secret     string   `short:"E" long:"secret" description:"database secret" `
		SecretKey  string
	}

	Generate struct {
		Name     string `short:"N" long:"name" description:"view DbName/route URI" `
		Location string `short:"X" long:"sqlx" description:"SQLX (extension for relation) location" `
	}

	Prepare struct {
		PrepareRule  string `short:"G" long:"generate" description:"prepare rule for patch|post|put"`
		ExecKind     string `long:"execKind" description:"allows to switch between service / dml"`
		DSQLOutput   string `long:"dqlOutput" description:"output path"`
		GoFileOutput string `long:"goFileOut" description:"destination of go file"`
		GoModulePkg  string `long:"goModulePkg" description:"go module package"`
		LoadPrevious bool   `long:"loadSQL" description:"decides whether to load records using "`
	}

	Plugins struct {
		PluginDst       string   `long:"pluginDst" description:"output plugin path"`
		PluginSrc       []string `long:"pluginSrc" description:"input plugin path"`
		PluginArgs      []string `long:"pluginArgs" description:"args need to be passed to generate a plugin"`
		PluginsURL      string   `long:"pluginsURL" description:"generated plugins destination"`
		PluginName      string   `long:"pluginName" description:"plugin name"`
		PluginGoVersion string   `long:"pluginGoVersion" description:"plugin go Version"`
		PluginOS        string   `long:"pluginOS" description:"plugin OS"`
		PluginArch      string   `long:"pluginArch" description:"plugin ARCH"`
	}

	Module struct {
		ModuleDst       string   `long:"moduleDst" description:"output module path"`
		ModuleSrc       []string `long:"moduleSrc" description:"input module path"`
		ModuleArgs      []string `long:"moduleArgs" description:"args need to be passed to generate a plugin"`
		ModuleName      string   `long:"moduleName" description:"module name"`
		ModuleMain      string   `long:"moduleMain" description:"module main"`
		ModuleLdFlags   string   `long:"moduleLdFlags" description:"module ldflags"`
		ModuleOS        string   `long:"moduleOS" description:"module OS"`
		ModuleArch      string   `long:"moduleArch" description:"plugin ARCH"`
		ModuleGoVersion string   `long:"moduleGoVersion" description:"module go Version"`
	}
)

//go:embed resource/mysql.json
var mysqlDev string

func (o *Options) BuildOption() *options.Options {
	var result = &options.Options{
		Version: o.Version,
	}
	prep := o.Prepare

	if prep.PrepareRule != "" {
		result.Generate = &options.Generate{
			Repository: options.Repository{},
			Rule:       options.Rule{},
			Dest:       prep.DSQLOutput,
			Operation:  prep.PrepareRule,
			Kind:       prep.ExecKind,
			Lang:       ast.LangVelty,
		}

	}
	if o.Location != "" {
		result.Translate = &options.Translate{
			Repository: options.Repository{},
			Rule:       options.Rule{},
		}
		if prep.PrepareRule != "" {
			result.Generate.Translate = true
		}
	}

	repo := result.Repository()
	if repo != nil {
		repo.RSA = o.JWTVerifierRSAKey
		repo.HMAC = o.JWTVerifierHMACKey
		repo.Port = &o.Port
		repo.RepositoryURL = o.WriteLocation
		repo.ConstURL = o.ConstURL
		repo.SubstitutesURL = o.SubstituesURL
		repo.Connector.Connectors = o.Connects
		if o.ApiURIPrefix == "" {
			o.ApiURIPrefix = "/v1/api"
		}
		repo.APIPrefix = o.ApiURIPrefix

		if o.PartialConfigURL != "" {
			fs := afs.New()
			if ok, _ := fs.Exists(context.Background(), o.PartialConfigURL); ok {
				repo.Configs.Append(o.PartialConfigURL)
			}
		}

	}

	if rule := result.Rule(); rule != nil {
		rule.ModuleLocation = o.RelativePath
		if rule.ModuleLocation == "" {
			rule.ModuleLocation = "pkg"
		}
		rule.Source = []string{o.Location}
		rule.ModulePrefix = o.RoutePrefix
		if o.GoModulePkg != "" {
			rule.Packages = []string{o.GoModulePkg}
		}
	}

	if o.ConfigURL != "" && repo == nil {
		result.Run = &options.Run{ConfigURL: o.ConfigURL, JobURL: o.JobURL}
	}
	return result
}
