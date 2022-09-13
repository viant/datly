package cmd

import (
	"context"
	_ "embed"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/view"
	"github.com/viant/scy"
	"strings"
)

type (
	Options struct {
		Port          int    `short:"p" long:"port" description:"port"  `
		RouteURL      string `short:"r" long:"route URL" description:"route URL"  `
		DependencyURL string `short:"d" long:"deps" description:"dependencies URL" `
		ConfigURL     string `short:"c" long:"config" description:"configuration URL" `
		JWTVerifier   string `short:"j" long:"jwt" description:"PublicKeyPath|EncKey" `
		WriteLocation string `short:"w" long:"write" description:"dump all config files to specified location" `
		Generate
		Connector
		Content
		CacheWarmup
		OpenApiURL string `short:"o" long:"openapi"`
		Version    bool   `short:"v" long:"version"  description:"build version" `
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
		Name         string   `short:"N" long:"name" description:"view DbName/route URI" `
		Table        string   `short:"T" long:"table" description:"table" `
		SQLXLocation string   `short:"X" long:"sqlx" description:"SQLX (extension for relation) location" `
		Relations    []string `short:"R" long:"relation" description:"relation in form of viewName:tableName" ` //TODO: remove
	}

	Content struct {
		Output         string `short:"O" long:"output" description:"output style" choice:"c" choice:"b" `
		RedirectSizeKb int    `short:"M" long:"redirect" description:"redirectMinSize" `
		RedirectURL    string `short:"L" long:"redirectURL" description:"redirectURL" `
	}
)

//go:embed resource/mysql.json
var mysqlDev string

func (o *Options) Init() {

	if o.SQLXLocation != "" {
		_, name := url.Split(o.SQLXLocation, file.Scheme)
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
	o.Connector.Init()
	switch o.Output {
	case "c":
		o.Output = "Comprehensive"
	default:
		o.Output = "Basic"
	}
}

func (c *Options) ResponseField() string {
	if c.Output == "Basic" {
		return ""
	}
	return "Data"
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
	if len(c.Connects) > 1 {
		for i := 1; i < len(c.Connects); i++ {
			parts := strings.Split(c.Connects[i], "|")
			if len(parts) < 3 {
				continue
			}
			conn := &view.Connector{
				Name:   parts[0],
				Driver: parts[1],
				DSN:    parts[2],
			}
			result[conn.Name] = conn
		}
	}
	return result
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
	if name == "" {
		name = o.Generate.Name
	}
	return "dev/" + name
}

func (o *Options) RouterURL() string {
	if o.Generate.Name == "" {
		return ""
	}
	return url.Join(o.RouteURL, o.RouterURI("")+".yaml")
}

func (o *Options) DepURL(uri string) string {
	if o.Generate.Name == "" {
		return ""
	}
	return url.Join(o.DependencyURL, uri+".yaml")
}

func (o *Options) SQLURL(name string, addSubFolder bool) string {
	pathSegments := []string{"dev"}
	if addSubFolder {
		location := o.SQLXLocation[strings.LastIndex(o.SQLXLocation, "/")+1:]
		extensionIndex := strings.LastIndex(location, ".")
		if extensionIndex != -1 {
			location = location[:extensionIndex]
		}
		pathSegments = append(pathSegments, location)
	}

	pathSegments = append(pathSegments, name+".sql")

	return url.Join(o.RouteURL, pathSegments...)
}

func (g *Generate) Namespace() string {
	if g.Table == "" {
		return ""
	}
	return namespace(g.Table)
}

func namespace(name string) string {
	parts := strings.Split(strings.ToLower(name), "_")
	if len(parts) > 2 {
		return parts[len(parts)-2][0:1] + parts[len(parts)-1][0:1]
	}
	return parts[len(parts)-1][0:2]
}
