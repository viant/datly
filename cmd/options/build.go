package options

import (
	"context"
	"github.com/viant/afs/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type GoBuild struct {
	Project   string   `short:"p" long:"proj" description:"project"`
	Module    string   `short:"m" long:"module" description:"custom go module location" default:"pkg" `
	Extension string   `short:"e" long:"ext" description:"extension replace project" default:".build/ext"`
	Datly     string   `short:"l" long:"xdatly" description:"custom extended datly location" default:".build/datly"`
	MainPath  string   `short:"M" long:"main" description:"main path"`
	Name      string   `short:"n" long:"name" description:"git module name" `
	Source    []string `short:"s" long:"source" description:"source locations"`
	DestURL   string   `short:"d" long:"dest" description:"dest location"`
	BuildArgs []string `short:"b" long:"buildArgs" description:"build args"`
	GoVersion string   `short:"v"  long:"goVer" description:"build go Version"`
	GoOs      string   `short:"o" long:"goOs" description:"plugin OS"`
	GoArch    string   `short:"a" long:"goArch" description:"plugin ARCH"`
	GoPath    string   `short:"P" long:"goPath" description:"go path"`
	GoRoot    string   `short:"R" long:"goRoot" description:"go root"`
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

	b.DestURL = ensureAbsPath(b.DestURL)

}

func (b *Build) Init() error {
	b.GoBuild.Init()
	if len(b.Source) == 0 {
		b.Source = append(b.Source, b.Datly, b.Module)
		if ok, _ := fs.Exists(context.Background(), b.Extension); ok {
			b.Source = append(b.Source, b.Extension)
		}

	}
	unixTs := time.Now().Unix()
	flags := "-X main.BuildTimeInS=" + strconv.Itoa(int(unixTs))
	if b.LdFlags == nil {
		b.LdFlags = &flags
	}
	if b.Runtime == "" {
		b.Runtime = "standalone"
	}

	switch b.Runtime {
	case "lambda/url":
		b.MainPath = "gateway/runtime/lambda/app/"
	case "lambda/apigw":
		b.MainPath = "gateway/runtime/apigw/app/"
	case "standalone":
		b.MainPath = "cmd/datly/"
	}
	return nil
}
