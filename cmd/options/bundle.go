package options

type Bundle struct {
	Source   string `short:"s" long:"src" description:"datly rule repository " `
	RuleDest string `short:"d" long:"dest" description:"datly rule repository rewrite deployment dest ie.: s3://vaint-e2e-config/datly-xxx/" `
}

func (b *Bundle) Init() error {
	b.Source = ensureAbsPath(b.Source)
	return nil
}
