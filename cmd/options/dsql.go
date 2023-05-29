package options

/*

	dsql,ext,build,deploy,gen,cache

	src
    dest
	datly dsql -s= -d=autogen
*/

type DSql struct {
	Connector
	JwtVerifier
	Generate
	Const       string `short:"C" long:"const" description:"const location" `
	Port        int    `short:"P" long:"port" description:"endpoint port" `
	RoutePrefix string `short:"f" long:"routePrefix" description:"routePrefix default: dev/"`
}
