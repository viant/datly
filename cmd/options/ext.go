package options

import (
	"github.com/viant/afs/url"
	"github.com/viant/toolbox/data"
	"os"
	"strings"
	"time"
)

type (
	Extension struct {
		Project string `short:"p" long:"proj" description:"destination project"`
		Module
		Datly
	}
	Module struct {
		Repository *string `short:"r" long:"module repo" description:"module repo"`
		Name       string  `short:"n" long:"name" description:"module name" default:"myapp"`
	}
	Datly struct {
		Location string `short:"x" long:"dsrc" description:"datly location" default:".build"`
		Tag      string `short:"t" long:"tag" description:" datly tag"`
	}
)

func (e *Extension) Init() error {
	if e.Project == "" {
		e.Project, _ = os.Getwd()
	}
	e.Project = ensureAbsPath(e.Project)
	if e.Datly.Location == "" {
		e.Datly.Location = url.Join(e.Project, ".build")
	}

	if e.Repository == nil {
		repo := "github.com/" + os.Getenv("USER")
		e.Repository = &repo
	}
	if e.Name == "" {
		e.Name = "myapp"
	}
	return nil
}

func (e *Module) Module() string {
	if e.Repository == nil {
		return e.Name
	}
	return *e.Repository + "/" + e.Name
}

func (e *Extension) Replacer(shared *Module) data.Map {
	var replacer = data.Map{}
	now := time.Now().UTC().Format(time.RFC3339)
	module := shared.Module()
	name := extractModuleName(module)

	replacer.Put("module", module)
	replacer.Put("moduleName", name)
	replacer.Put("modulePath", url.Join(e.Project, "pkg"))
	replacer.Put("extModulePath", url.Join(e.Project, ".build/ext"))
	replacer.Put("generatedAt", now)
	return replacer
}

func extractModuleName(name string) string {
	if index := strings.LastIndex(name, "."); index != -1 {
		name = name[index+1:]
	}
	if index := strings.LastIndex(name, "/"); index != -1 {
		name = name[index+1:]
	}
	return name
}

func (e *Extension) GoModInitArgs(shared *Module) []string {
	return []string{
		"mod",
		"init",
		shared.Module(),
	}
}
