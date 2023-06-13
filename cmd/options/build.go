package options

import (
	"context"
	"github.com/viant/afs/url"
	"os"
	"runtime"
	"strings"
)

type GoBuild struct {
	Project   string `short:"p" long:"proj" description:"project"`
	Module    string `short:"m" long:"module" description:"custom go module location" default:"pkg" `
	Extension string `short:"e" long:"ext" description:"extension replace project" default:".build/ext"`
	Datly     string `short:"l" long:"xdatly" description:"custom extended datly location" default:".build/datly"`

	Source    []string `short:"s" long:"source" description:"source locations"`
	Dest      string   `short:"d" long:"dest" description:"dest location"`
	BuildArgs []string `short:"b" long:"buildArgs" description:"build args"`
	GoVersion string   `short:"v"  long:"goVer" description:"build go Version"`
	GoOs      string   `short:"o" long:"goOs" description:"plugin OS"`
	GoArch    string   `short:"a" long:"goArch" description:"plugin ARCH"`
}

type Build struct {
	GoBuild
	LdFlags *string `short:"f" long:"ldflags" description:"build ldflags"`
	Runtime string  `short:"r" long:"runtime" description:"runtime binary" choice:"standalone" choice:"lambda/url" choice:"lambda/apigw"`
}

func (b *GoBuild) Init() {
	if b.Project == "" {
		b.Project, _ = os.Getwd()
	}
	b.Project = ensureAbsPath(b.Project)

	if b.Module == "" {
		b.Module = "pkg"
	}
	if url.IsRelative(b.Module) {
		b.Module = url.Join(b.Project, b.Module)
	}

	if b.Datly == "" {
		b.Datly = ".build/datly"
	}
	if url.IsRelative(b.Datly) {
		b.Datly = url.Join(b.Project, b.Datly)
	}

	if b.Extension == "" {
		b.Extension = ".build/ext"
	}
	if url.IsRelative(b.Extension) {
		b.Extension = url.Join(b.Project, b.Extension)
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

	b.Dest = ensureAbsPath(b.Dest)
}

func (b *Build) Init() error {
	b.GoBuild.Init()
	if len(b.Source) == 0 {
		b.Source = append(b.Source, b.Datly, b.Module)
		if ok, _ := fs.Exists(context.Background(), b.Extension); ok {
			b.Source = append(b.Source, b.Extension)
		}

	}
	flags := "-X main.BuildTimeInS=`date +%s`"
	if b.LdFlags == nil {
		b.LdFlags = &flags
	}
	return nil
}
