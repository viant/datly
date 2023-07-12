package options

import "context"

type Options struct {
	InitExt   *Extension   `command:"initExt" description:"initialises datly extension project" `
	Build     *Build       `command:"build" description:"build custom datly binary"  `
	Plugin    *Plugin      `command:"plugin" description:"build custom datly rule plugin"  `
	Generate  *Generate    `command:"gen" description:"generate dsql for put,patch or post operation" `
	Translate *Translate   `command:"dsql" description:"converts dsql into datly rule"`
	Cache     *CacheWarmup `command:"cache" description:"warmup cache"`
	Run       *Run         `command:"run" description:"start datly in standalone mode"`
	Bundle    *Bundle      `command:"bundle" description:"bundles rules for cloud deployment (speed/cost optimization)"`
	InitCmd   *Init        `command:"init" description:"init datly rule repository"`
	Touch     *Touch       `command:"touch" description:"forces route rule sync"`
	Legacy    bool         `short:"l" long:"legacy" description:"show legacy datly option"`
}

func (o *Options) Connectors() []string {
	if candidate := o.Generate; candidate != nil && len(candidate.Connectors) > 0 {
		return candidate.Connectors
	} else if candidate := o.Translate; candidate != nil && len(candidate.Connectors) > 0 {
		return candidate.Connectors
	}
	return []string{}
}

func (o *Options) ConfigURL() string {
	if o.Translate != nil {
		return o.Translate.ConfigURL
	}
	if o.Generate != nil {
		return o.Generate.ConfigURL
	}
	if o.Run != nil {
		return o.Run.ConfigURL
	}
	return ""
}

func (o *Options) Init(ctx context.Context) error {
	if o.InitExt != nil {
		return o.InitExt.Init()
	}
	if o.Build != nil {
		return o.Build.Init()
	}
	if o.Plugin != nil {
		return o.Plugin.Init()
	}
	if o.Generate != nil {
		return o.Generate.Init()
	}
	if o.Translate != nil {
		return o.Translate.Init(ctx)
	}
	if o.Run != nil {
		return o.Run.Init()
	}
	if o.Bundle != nil {
		return o.Bundle.Init()
	}
	if o.InitCmd != nil {
		return o.InitCmd.Init(ctx)
	}
	if o.Touch != nil {
		o.Touch.Init()
	}
	return nil
}

func NewOptions(args Arguments) *Options {
	ret := &Options{}
	switch args[0] {
	case "plugin":
		ret.Plugin = &Plugin{}
	case "build":
		ret.Build = &Build{}
	case "initExt":
		ret.InitExt = &Extension{}
	case "gen":
		ret.Generate = &Generate{}
	case "init":
		ret.InitCmd = &Init{}
	case "dsql":
		ret.Translate = &Translate{}
	case "cache":
		ret.Cache = &CacheWarmup{}
	case "run":
		ret.Run = &Run{}
	case "bundle":
		ret.Bundle = &Bundle{}
	case "touch":
		ret.Touch = &Touch{}
	}
	return ret
}

func (o *Options) Repository() *Repository {
	if o.Translate != nil {
		return &o.Translate.Repository
	}
	if o.Generate != nil {
		return &o.Generate.Repository
	}
	return nil
}

func (o *Options) Rule() *Rule {
	if o.Translate != nil {
		return &o.Translate.Rule
	}
	if o.Generate != nil {
		return &o.Generate.Rule
	}
	return nil
}
