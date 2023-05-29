package options

type Init struct {
	Dest  string `short:"d" long:"dest" description:"datly rule project destination" `
	Const string `short:"C" long:"const" description:"const location" `
	Port  string `short:"P" long:"port" description:"endpoint port" `
	Connector
	JwtVerifier
}
