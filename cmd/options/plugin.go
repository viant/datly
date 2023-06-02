package options

import (
	"github.com/viant/afs/url"
	"strings"
)

type Plugin struct {
	GoBuild
	Repo string `short:"r" long:"repo" description:"rule repository location"`
}

func (p *Plugin) Init() error {
	p.GoBuild.Init()
	if len(p.Source) == 0 {
		p.Source = append(p.Source, p.Module)
	}
	p.Repo = ensureAbsPath(p.Repo)
	if p.Dest == "" && p.Repo != "" {
		p.Dest = url.Join(p.Repo, "Datly/plugins")
	}
	return nil
}

func (p *Plugin) RouteURL() string {
	return url.Join(p.Repo, "Datly/routes")
}

func (p *Plugin) Touch() *Touch {
	return &Touch{RoutesURL: p.RouteURL(), Repo: p.Repo}
}
func (p *Plugin) IsRepositoryPlugin() bool {
	return p.Repo != "" && strings.HasPrefix(p.Dest, p.Repo)
}
