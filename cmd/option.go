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
		RouteURL      string `short:"r" long:"mode" description:"route URL"  `
		DependencyURL string `short:"d" long:"deps" description:"dependencies URL" `
		ConfigURL     string `short:"c" long:"config" description:"configuration URL" `
		WriteLocation string `short:"w" long:"write" description:"dump all config files to specified location" `
		Generate
		Connector
		Content
		OpenApiURL string `short:"o" long:"openapi"`
	}

	Connector struct {
		DbName string `short:"C" long:"dbname" description:"db/connector name" `
		Driver string `short:"D" long:"driver" description:"driver" `
		DSN    string `short:"A" long:"dsn" description:"DSN" `
		Secret string `short:"E" long:"secret" description:"database secret" `
	}

	Generate struct {
		Name         string   `short:"N" long:"name" description:"view DbName/route URI" `
		Table        string   `short:"T" long:"table" description:"table" `
		SQLLocation  string   `short:"S" long:"sql" description:"SQL location" `
		SQLXLocation string   `short:"X" long:"sqlx" description:"SQLX (extension for relation) location" `
		Relations    []string `short:"R" long:"relation" description:"relation in form of viewName:tableName" `
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

func (c *Connector) Init() {
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
	if c.Secret == "" {
		c.Secret = "mem://localhost/resource/mysql.json"
		fs := afs.New()
		fs.Upload(context.Background(), c.Secret, file.DefaultFileOsMode, strings.NewReader(mysqlDev))
	}
}

func (c *Connector) New() *view.Connector {
	result := &view.Connector{
		Name:   c.DbName,
		Driver: c.Driver,
		DSN:    c.DSN,
	}
	result.Secret = &scy.Resource{
		Name: "",
		URL:  c.Secret,
		Key:  "blowfish://default",
		Data: nil,
	}
	return result
}

func (o *Options) RouterURI() string {
	return "dev/" + o.Generate.Name
}

func (o *Options) RouterURL() string {
	if o.Generate.Name == "" {
		return ""
	}
	return url.Join(o.RouteURL, o.RouterURI()+".yaml")
}

func (o *Options) DepURL(uri string) string {
	if o.Generate.Name == "" {
		return ""
	}
	return url.Join(o.DependencyURL, uri+".yaml")
}

func (o *Options) SQLURL(name string) string {
	return url.Join(o.RouteURL, "dev/"+name+".sql")
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
