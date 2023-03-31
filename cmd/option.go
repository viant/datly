package cmd

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cmd/option"
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
		Port               int    `short:"p" long:"port" description:"port"  `
		RouteURL           string `short:"r" long:"routeURL" description:"route URL"  `
		CustomRouterURL    string `long:"routerConfig" description:"custom router template/config URL"`
		DependencyURL      string `short:"d" long:"deps" description:"dependencies URL" `
		ConfigURL          string `short:"c" long:"config" description:"configuration URL" `
		PartialConfigURL   string `short:"e" long:"partialConfig" description:"partial configuration file URL"`
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
	}

	Package struct {
		RuleSourceURL string `short:"P" long:"packageSrc" description:"package rule source URL " `
		RuleDestURL   string `short:"R" long:"packageDest" description:"package rule destination URL rewrite" `
	}

	CacheWarmup struct {
		WarmupURIs []string `short:"u" long:"wuri" description:"uri to warmup cache" `
	}

	Connector struct {
		Connects []string `short:"C" long:"conn" description:"name|driver|dsn" `
		DbName   string   `short:"V" long:"dbname" description:"db/connector name" `
		Driver   string   `short:"D" long:"driver" description:"driver" `
		DSN      string   `short:"A" long:"dsn" description:"DSN" `
		Secret   string   `short:"E" long:"secret" description:"database secret" `
	}

	Generate struct {
		Name     string `short:"N" long:"name" description:"view DbName/route URI" `
		Location string `short:"X" long:"sqlx" description:"SQLX (extension for relation) location" `
	}

	Prepare struct {
		PrepareRule  string `short:"G" long:"generate" description:"prepare rule for patch|post|put|delete"`
		ExecKind     string `long:"execKind" description:"allows to switch between sql / dml"`
		DSQLOutput   string `long:"dsqlOutput" description:"output path"`
		GoFileOutput string `long:"goFileOut" description:"destination of go file"`
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

	if !strings.HasPrefix(o.DSQLOutput, "/") {
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

	o.Connector.Init()

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

func (c *Connector) Init() {

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
		}
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
	result := []*view.Connector{
		{
			Name:   c.DbName,
			Driver: c.Driver,
			DSN:    c.DSN,
		},
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

func namespace(name string) string {
	parts := strings.Split(strings.ToLower(name), "_")
	if len(parts) > 2 {
		return parts[len(parts)-2][0:1] + parts[len(parts)-1][0:1]
	}

	return parts[len(parts)-1][0:2]
}
