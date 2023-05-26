package options

import (
	"github.com/viant/afs/url"
	"os"
)

type (
	Extension struct {
		Datly
		Mode      string
		Name      string   `short:"n" long:"name" description:"module name"`
		Revision  string   `short:"r" long:"rev" description:"datly revision"`
		Source    []string `short:"s" long:"source" description:"source locations"`
		Dest      string   `short:"d" long:"dest" description:"destination"`
		BuildArgs []string `short:"b" long:"buildArgs" description:"build args"`
		GoVersion string   `short:"v"  long:"goVer" description:"build go Version"`
		GoOs      string   `short:"o" long:"goOs" description:"plugin OS"`
		GoArch    string   `short:"a" long:"goArch" description:"plugin ARCH"`
	}
	Datly struct {
		Location string `short:"x" long:"x" description:"datly location"`
		Tag      string `short:"t" long:"t" description:" datly tag"`
	}
)

func (e *Extension) Init() error {
	if e.Dest == "" {
		e.Dest, _ = os.Getwd()
	}
	if e.Datly.Location == "" {
		e.Datly.Location = url.Join(e.Dest, ".xdatly")
	}
	return nil
}

/*
https://github.com/viant/datly/archive/refs/tags/v0.5.5.0.zip
*/
/*
 - init
 - plugin


 --buildMode='plugin'
--pluginSrc='${appPath}/pkg'
--pluginDst='${appPath}/e2e/autogen/Datly/cloud_plugins/'
--pluginArgs='-trimpath'
--pluginOS='linux'
--pluginArch='amd64



 - build -       -
		--buildMode='exec'
		--moduleName=datly
		--moduleMain=cmd/datly
		--moduleSrc='${appPath}/.build/datly'
		--moduleSrc='${appPath}/pkg'
		--moduleDst='/tmp/xdatly'
		--moduleLdFlags='-X main.BuildTimeInS=`date +%s`'
		--moduleArgs='-trimpath'
		--moduleOS='${os.system}'
		--moduleArch='${os.architecture}'

*/
