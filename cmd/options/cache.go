package options

type Cache struct {
	WarmupURIs []string `short:"u" long:"wuri" description:"uri to warmup cache" `
	ConfigURL  string   `short:"c" long:"conf" description:"datly config" `
}
