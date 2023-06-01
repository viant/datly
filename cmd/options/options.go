package options

type Options struct {
	InitExt  *Extension `command:"initExt" description:"initialises datly extension project" `
	Build    *Build     `command:"build" description:"build custom datly binary"  `
	Plugin   *Plugin    `command:"plugin" description:"build custom datly rule plugin"  `
	Generate *Gen       `command:"gen" description:"generate dsql for put,patch or post operation" `
	DSql     *DSql      `command:"dsql" description:"converts dsql into datly rule"`
	Cache    *Cache     `command:"cache" description:"warmup cache"`
	Run      *Run       `command:"run" description:"start datly in standalone mode"`
	Bundle   *Bundle    `command:"bundle" description:"bundles rules for cloud deployment (speed/cost optimization)"`
	InitCmd  *Init      `command:"init" description:"init datly rule repository"`
	Legacy   bool       `short:"l" long:"legacy" description:"show legacy datly option"`
}

func (o *Options) Init() error {
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
	if o.DSql != nil {
		return o.DSql.Init()
	}
	if o.Run != nil {
		return o.Run.Init()
	}
	if o.Bundle != nil {
		return o.Bundle.Init()
	}
	if o.InitCmd != nil {
		return o.InitCmd.Init()
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
		ret.Generate = &Gen{}
	case "init":
		ret.InitCmd = &Init{}
	case "dsql":
		ret.DSql = &DSql{}
	case "cache":
		ret.Cache = &Cache{}
	case "run":
		ret.Run = &Run{}
	case "bundle":
		ret.Bundle = &Bundle{}
	}
	return ret
}
