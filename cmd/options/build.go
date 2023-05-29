package options

import (
	"github.com/viant/afs/url"
	"os"
	"path"
	"runtime"
	"strings"
)

type GoBuild struct {
	Project   string   `short:"p" long:"proj" description:"project"`
	Source    []string `short:"s" long:"source" description:"source locations"`
	Dest      string   `short:"d" long:"dest" description:"dest location"`
	BuildArgs []string `short:"b" long:"buildArgs" description:"build args"`
	GoVersion string   `short:"v"  long:"goVer" description:"build go Version"`
	GoOs      string   `short:"o" long:"goOs" description:"plugin OS"`
	GoArch    string   `short:"a" long:"goArch" description:"plugin ARCH"`
}

type Build struct {
	Plugin
	LdFlags *string `short:"f" long:"ldflags" description:"build ldflags"`
	Runtime string  `short:"r" long:"runtime" description:"runtime binary" choice:"standalone" choice:"lambda/url" choice:"lambda/apigw"`
}

func (b *GoBuild) Init() {
	if b.Project == "" {
		b.Project, _ = os.Getwd()
	}
	if len(b.BuildArgs) == 0 {
		b.BuildArgs = append(b.BuildArgs, "-trimpath")
	}
	if b.GoOs == "" {
		b.GoOs = runtime.GOOS
	}
	if b.GoArch == "" {
		b.GoArch = runtime.GOARCH
	}
	if b.GoVersion == "" {
		b.GoVersion = strings.Replace(runtime.Version(), "go", "", 1)
	}
	if b.Dest != "" && url.IsRelative(b.Dest) {
		b.Dest = url.Join(b.Project, b.Dest)
	}
}

func (b *Build) Init() error {
	b.GoBuild.Init()
	if len(b.Source) == 0 {
		b.Source = append(b.Source, path.Join(b.Project, ".build/datly"), path.Join(b.Project, "pkg"))
	}
	flags := "-X main.BuildTimeInS=`date +%s`"
	if b.LdFlags == nil {
		b.LdFlags = &flags
	}
	return nil
}
