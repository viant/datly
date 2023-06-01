package options

type Connector struct {
	Connectors []string `short:"c" long:"conn" description:"name|driver|dsn|secretUrl|key" `
}

func (c *Connector) Init() {
	if len(c.Connectors) == 0 {
		return
	}
	for i, con := range c.Connectors {
		c.Connectors[i] = expandHomeDir(con)
	}
}
