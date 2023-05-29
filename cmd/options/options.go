package options

type Options struct {
	InitExt  *Extension `command:"initExt" description:"initialises datly extension project" `
	Build    *Build     `command:"build" description:"build custom datly binary"  `
	Plugin   *Plugin    `command:"plugin" description:"build custom datly rule plugin"  `
	Generate *Gen       `command:"gen" description:"generate dsql for put,patch or post operation" `
	DSql     *DSql      `command:"dsql" description:"converts dsql into datly rule"`
	Cache    *Cache     `command:"cache" description:"warmup cache"`
	Run      *Run       `command:"run" description:"start datly in standalone mode"`
	//InitCmd  *Init      `command:"init" description:"initialises datly rule project"`
	Legacy bool `short:"l" long:"legacy" description:"show legacy datly option"`
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
	if o.Run != nil {
		return o.Run.Init()
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
	case "dsql":
		ret.DSql = &DSql{}
	case "cache":
		ret.Cache = &Cache{}
	case "run":
		ret.Run = &Run{}
		//case "init":
		//	ret.InitCmd = &Init{}

	}
	return ret
}
