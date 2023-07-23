package options

import (
	"github.com/viant/afs/url"
	"strings"
)

type Plugin struct {
	GoBuild
	Repository string `short:"r" long:"repo" description:"rule repository location"`
}

func (p *Plugin) Init() error {
	p.GoBuild.Init()
	if len(p.Source) == 0 {
		p.Source = append(p.Source, p.Module)
	}
	expandRelativeIfNeeded(&p.Repository, p.Project)
	if p.DestURL == "" && p.Repository != "" {
		p.DestURL = url.Join(p.Repository, "Datly/plugins")
	}
	return nil
}

func (p *Plugin) RouteURL() string {
	return url.Join(p.Repository, "Datly/routes")
}

func (p *Plugin) Touch() *Touch {
	return &Touch{RoutesURL: p.RouteURL(), Repo: p.Repository}
}
func (p *Plugin) IsRepositoryPlugin() bool {
	return p.Repository != "" && strings.HasPrefix(p.DestURL, p.Repository)
}
