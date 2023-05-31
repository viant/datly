package options

type Init struct {
	Dest  string `short:"d" long:"dest" description:"datly rule repository location" `
	Const string `short:"C" long:"const" description:"const location" `
	Port  string `short:"P" long:"port" description:"endpoint port" default:"8080"`
	Connector
	JwtVerifier
}
