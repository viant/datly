package options

type Mbus struct {
	MBuses []string `short:"b" long:"mbus" description:"name|vendor|resourceType|uri[|region|secretURL|secretKey]" `
}

func (c *Mbus) Init() {
	if len(c.MBuses) == 0 {
		return
	}
	for i, con := range c.MBuses {
		c.MBuses[i] = expandHomeDir(con)
	}
}
