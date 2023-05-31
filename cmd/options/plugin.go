package options

type Plugin struct {
	GoBuild
}

func (p *Plugin) Init() error {
	p.GoBuild.Init()
	if len(p.Source) == 0 {
		p.Source = append(p.Source, p.Module)
	}
	return nil
}
