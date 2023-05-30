package options

type Bundle struct {
	Source   string `short:"s" long:"src" description:"runtime rule dalty project " `
	RuleDest string `short:"d" long:"dest" description:"datly rule rewrite dest ie.: s3://vaint-e2e-config/datly-xxx/" `
}

func (b *Bundle) Init() error {
	b.Source = ensureAbsPath(b.Source)
	return nil
}
