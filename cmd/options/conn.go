package options

type Connector struct {
	Connectors []string `short:"c" long:"conn" description:"name|driver|dsn|secretUrl|key" `
}
