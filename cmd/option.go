package cmd

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/internal/codegen/ast"
	"github.com/viant/datly/view"
	"github.com/viant/scy"
	"path"
	"runtime"
	"strings"
)

const (
	PreparePost   = "post"
	PreparePut    = "put"
	PreparePatch  = "patch"
	PrepareDelete = "delete"
	APIPrefix     = "/v1/api/"

	//folderDev = "dev"
	folderSQL = "dsql"

	rootFolder          = "Datly"
	buildModePluginless = "pluginless"
	buildModePlugin     = "plugin"
	buildModeExec       = "exec"
)

type (
	Options struct {
		Port               int `short:"p" long:"port" description:"port"  `
		hasPort            bool
		RouteURL           string `short:"r" long:"routeURL" description:"route URL"  `
		CustomRouterURL    string `long:"routerConfig" description:"custom router template/config URL"`
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
		AssetsURL string `short:"a" long:"assetsURL" description:"assets destination"`
		ConstURL  string `long:"constURL" description:"path where const files are stored"`
		Legacy    bool   `short:"l"`
		cache     *view.Cache
		EnvURL    string `long:"envURL" description:"environment url, expands template before processing"`
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
		DSQLOutput   string `long:"dsqlOutput" description:"output path"`
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

func (o *Options) GoModuleLocation() string {
	if o.RelativePath != "" {
		return o.RelativePath
	}
	if o.GoModulePkg != "" {
		return o.GoModulePkg
	}
	return o.DSQLOutput
}
func (c *Connector) SetConnectors(connectors []*view.Connector) {
	if len(connectors) == 0 {
		return
	}
	var merged = make([]*view.Connector, 0, len(connectors))
	c.DbName = connectors[0].Name
	c.Driver = connectors[0].Driver
	c.DSN = connectors[0].DSN
	if connectors[0].Secret != nil {
		c.Secret = connectors[0].Secret.URL
		c.SecretKey = connectors[0].Secret.Key
	}
	var index = map[string]bool{
		c.DbName: true,
	}
	merged = append(merged, connectors[0])
	for i := 1; i < len(connectors); i++ {
		connector := connectors[i]
		if index[connector.Name] {
			continue
		}
		index[connector.Name] = true
		merged = append(merged, connector)
	}
	c.connectors = merged
}

//go:embed resource/mysql.json
var mysqlDev string

func (o *Options) Init() error {
	if o.Location != "" {
		_, name := url.Split(o.Location, file.Scheme)
		if index := strings.Index(name, "."); index != -1 {
			name = name[:index]
		}
		o.Name = name
	}

	if o.ConfigURL != "" {
		o.ConfigURL = normalizeURL(o.ConfigURL)
	}

	if o.RouteURL != "" {
		o.RouteURL = normalizeURL(o.RouteURL)
	}

	if o.DependencyURL != "" {
		o.DependencyURL = normalizeURL(o.DependencyURL)
	}

	if o.ExecKind == "" {
		o.ExecKind = option.ExecKindService
	}

	if o.DSQLOutput == "" {
		o.DSQLOutput = folderSQL
	}

	if url.IsRelative(o.DSQLOutput) {
		o.DSQLOutput = path.Join(path.Dir(o.Location), o.DSQLOutput)
	}

	if o.PluginsURL == "" {
		o.PluginsURL = "plugins"
	}
	if o.RoutePrefix == "" {
		o.RoutePrefix = "dev"
	}
	if o.ApiURIPrefix == "" {
		o.ApiURIPrefix = APIPrefix
	}
	o.PluginsURL = path.Join(o.WriteLocation, rootFolder, o.PluginsURL)
	o.PrepareRule = strings.ToLower(o.PrepareRule)

	if o.PluginArch == "" {
		o.PluginArch = runtime.GOARCH
	}

	if o.PluginOS == "" {
		o.PluginOS = runtime.GOOS
	}

	if o.PluginGoVersion == "" {
		o.PluginGoVersion = strings.Replace(runtime.Version(), "go", "", 1)
	}

	if o.BuildMode == buildModePlugin {
		if len(o.PluginSrc) == 0 {
			return fmt.Errorf("PluginSrc can't be empty")
		}

		if o.PluginDst == "" {
			return fmt.Errorf("PluginDst can't be empty")
		}
	}

	if o.BuildMode == buildModeExec {
		if o.Module.ModuleDst == "" {
			return fmt.Errorf("ModuleDst can't be empty")
		}

		if len(o.Module.ModuleSrc) == 0 {
			return fmt.Errorf("ModuleSrc can't be empty")
		}

		if o.ModuleMain == "" {
			return fmt.Errorf("ModuleMain can't be empty")
		}

		if o.ModuleMain == "" {
			base := path.Base(o.ModuleMain)
			ext := path.Ext(base)
			base = strings.Replace(base, ext, "", 1)
			o.ModuleName = base
		}
		if o.ModuleGoVersion == "" {
			o.ModuleGoVersion = strings.Replace(runtime.Version(), "go", "", 1)
		}

		if o.ModuleArch == "" {
			o.ModuleArch = runtime.GOARCH
		}

		if o.PluginOS == "" {
			o.ModuleOS = runtime.GOOS
		}
	}

	o.Connector.Init(o.PartialConfigURL == "")

	return nil
}

func (o *Options) IsPluginlessBuildMode() bool {
	return o.BuildMode == buildModePluginless
}

func (o *Options) IsPluginBuildMode() bool {
	return o.BuildMode == buildModePlugin
}

func (o *Options) IsExecBuildMode() bool {
	return o.BuildMode == buildModeExec
}

// MatchConnector returns matcher or default connector
func (c *Connector) MatchConnector(name string) *view.Connector {
	if name == "" {
		return c.New()
	}
	registry := c.Registry()
	if result, ok := registry[name]; ok {
		return result
	}
	return c.New()
}

func (c *Connector) Init(fallbackToDefault bool) {

	if len(c.Connects) > 0 {
		parts := strings.Split(c.Connects[0], "|")
		switch len(parts) {
		case 1:
			c.DbName = parts[0]
		case 2:
			c.DbName = parts[0]
			c.Driver = parts[1]
		case 3:
			c.DbName = parts[0]
			c.Driver = parts[1]
			c.DSN = parts[2]
		case 4:
			c.DbName = parts[0]
			c.Driver = parts[1]
			c.DSN = parts[2]
			c.Secret = parts[3]
		case 5:
			c.DbName = parts[0]
			c.Driver = parts[1]
			c.DSN = parts[2]
			c.Secret = parts[3]
			c.SecretKey = parts[4]

		}
	}
	if !fallbackToDefault {
		return
	}

	if c.Driver == "" {
		c.Driver = "mysql"
	}
	if c.DSN == "" {
		name := c.DbName
		if name == "" {
			name = "dev"
		}
		c.DSN = "${Username}:${Password}@tcp(localhost:3306)/" + name + "?parseTime=true"
	}
	if c.DbName == "" {
		c.DbName = "dev"
	}
	if c.Secret == "" && (strings.Contains(c.DSN, "localhost")) {
		c.Secret = "mem://localhost/resource/mysql.json"
		fs := afs.New()
		fs.Upload(context.Background(), c.Secret, file.DefaultFileOsMode, strings.NewReader(mysqlDev))
	}
}

func (c *Connector) Registry() map[string]*view.Connector {
	var result = map[string]*view.Connector{}
	defaultConn := c.New()
	result[defaultConn.Name] = defaultConn
	connectors := c.Connectors()
	for i := range connectors {
		result[connectors[i].Name] = connectors[i]
	}

	return result
}

func (c *Connector) Connectors() []*view.Connector {
	if len(c.connectors) > 0 {
		return c.connectors
	}
	c.connectors = c.decodeConnectors()
	return c.connectors
}

func (c *Connector) decodeConnectors() []*view.Connector {
	var secret *scy.Resource
	if c.Secret != "" {
		secret = &scy.Resource{URL: c.Secret}
		if c.SecretKey != "" {
			secret.Key = c.SecretKey
		}
	}
	result := []*view.Connector{}
	if c.DSN != "" {
		result = append(result, &view.Connector{
			Name:   c.DbName,
			Driver: c.Driver,
			DSN:    c.DSN,
			Secret: secret,
		})
	}
outer:
	for i := 0; i < len(c.Connects); i++ {
		parts := strings.Split(c.Connects[i], "|")
		if len(parts) < 3 {
			continue
		}
		conn := &view.Connector{
			Name:   parts[0],
			Driver: parts[1],
			DSN:    parts[2],
		}

		switch len(parts) {
		case 4:
			conn.Secret = &scy.Resource{URL: parts[3]}
		case 5:
			conn.Secret = &scy.Resource{URL: parts[3], Key: parts[4]}
		}

		for _, connector := range result {
			if connector.Name == conn.Name {
				continue outer
			}
		}

		result = append(result, conn)
	}

	return result
}

func (c *Connector) Lookup(connectorName string) (*view.Connector, bool) {
	conn, ok := c.Registry()[connectorName]
	return conn, ok
}

func (c *Connector) New() *view.Connector {
	result := &view.Connector{
		Name:   c.DbName,
		Driver: c.Driver,
		DSN:    c.DSN,
	}

	if c.Secret != "" {
		result.Secret = &scy.Resource{
			Name: "",
			URL:  c.Secret,
			Data: nil,
		}
		if result.Secret.Key == "" && c.Driver == "mysql" {
			result.Secret.Key = "blowfish://default"
		}
	}
	return result
}

func (o *Options) RouterURI(name string) string {
	return combineURLs(o.RoutePrefix, name)
}

func (o *Options) RouterURL(fileName string) string {
	return url.Join(o.RouteURL, o.RouterURI(fileName)+".yaml")
}

func (o *Options) DepURL(uri string) string {
	return url.Join(o.DependencyURL, uri+".yaml")
}

func (o *Options) MergeFromBuild(build *options.Build) {
	o.BuildMode = "exec"
	o.ModuleName = "datly"
	switch build.Runtime {
	case "lambda/url":
		o.ModuleMain = "gateway/runtime/lambda/app/"
	case "lambda/apigw":
		o.ModuleMain = "gateway/runtime/apigw/app/"
	case "standalone":
		o.ModuleMain = "cmd/datly/"
	}
	o.ModuleSrc = build.Source
	o.ModuleDst = build.Dest
	o.ModuleLdFlags = *build.LdFlags
	o.ModuleArgs = build.BuildArgs
	o.ModuleOS = build.GoOs
	o.ModuleArch = build.GoArch
	o.ModuleGoVersion = build.GoVersion
}

func (o *Options) MergeFromPlugin(plugin *options.Plugin) {
	o.BuildMode = "plugin"
	o.PluginSrc = plugin.Source
	o.PluginDst = plugin.Dest
	o.PluginArgs = plugin.BuildArgs
	o.PluginOS = plugin.GoOs
	o.PluginArch = plugin.GoArch
	o.PluginGoVersion = plugin.GoVersion
}

func (o *Options) MergeFromGenerate(generate *options.Gen) {
	o.Connects = generate.Connectors
	o.PrepareRule = generate.Operation
	o.ExecKind = generate.Kind
	o.Name = generate.Name
	o.Generate.Location = generate.Source
	if generate.Module != "" {
		o.GoFileOutput = generate.Module
		o.RelativePath = generate.Module
	}
	o.GoModulePkg = generate.Package
	o.DSQLOutput = generate.Dest
}

func (o *Options) MergeFromCache(cache *options.CacheWarmup) {
	o.CacheWarmup.WarmupURIs = cache.URIs
	o.ConfigURL = cache.ConfigURL
}

func (o *Options) MergeFromRun(run *options.Run) {
	o.ConfigURL = run.ConfigURL
}

func (o *Options) MergeFromDSql(dsql *options.DSql) {
	o.WriteLocation = dsql.Repo
	o.Name = dsql.Name
	o.Location = dsql.Source
	o.Connects = dsql.Connectors
	o.JWTVerifierHMACKey = string(dsql.JwtVerifier.HMAC)
	o.JWTVerifierRSAKey = string(dsql.JwtVerifier.RSA)
	o.ConstURL = dsql.Const
	if dsql.Port != nil {
		o.Port = *dsql.Port
		o.hasPort = true
	}
	o.RoutePrefix = dsql.RoutePrefix
	if dsql.Module != "" {
		o.RelativePath = dsql.Module
	}
	if dsql.Port == nil {
		o.PartialConfigURL = dsql.ConfigURL
		o.RouteURL = url.Join(dsql.Repo, "Datly/routes")
	}
}

func (o *Options) MergeFromInit(init *options.Init) {
	o.isInit = true
	o.Connects = init.Connectors
	o.JWTVerifierHMACKey = string(init.JwtVerifier.HMAC)
	o.JWTVerifierRSAKey = string(init.JwtVerifier.RSA)
	if init.Port != nil {
		o.Port = *init.Port
	}
	o.ConstURL = init.Const
	o.WriteLocation = init.Repo
	o.PartialConfigURL = init.ConfigURL
	if init.CacheProvider.ProviderURL != "" {
		o.cache = &view.Cache{
			Name:         init.Name,
			Location:     init.Location,
			Provider:     init.ProviderURL,
			TimeToLiveMs: init.TimeToLiveMs,
		}
	}
}

func (o *Options) BuildOption() *options.Options {
	var result = &options.Options{}
	prep := o.Prepare
	if prep.PrepareRule != "" {
		result.Generate = &options.Gen{
			Connector: options.Connector{
				Connectors: o.Connects,
			},
			Generate: options.Generate{
				Module: o.RelativePath,
				Source: o.Location,
			},
			Package:   o.GoModulePkg,
			Dest:      prep.DSQLOutput,
			Operation: prep.PrepareRule,
			Kind:      prep.ExecKind,
			Lang:      ast.LangVelty,
		}

	}

	return result
}

func namespace(name string) string {
	parts := strings.Split(strings.ToLower(name), "_")
	if len(parts) > 2 {
		return parts[len(parts)-2][0:1] + parts[len(parts)-1][0:1]
	}

	return parts[len(parts)-1][0:2]
}
