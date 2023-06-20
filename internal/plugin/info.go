package plugin

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"golang.org/x/mod/modfile"
)

const (
	xDatlyTypeCustomModule     = "github.com/viant/xdatly/types/custom"
	xDatlyTypesExtensionModule = "github.com/viant/xdatly/extension"
)

type Info struct {
	fs           afs.Service
	URL          string
	Mod          *modfile.File
	IsExtension  bool
	IsCustom     bool
	DefineMethod bool
}

func (i *Info) IsStandalone() bool {
	return i.Mod == nil
}

func (i *Info) IsDatlyExtension() bool {
	return i.IsExtension || i.IsCustom
}

func (i *Info) init(ctx context.Context) error {
	if err := i.tryLoadModFile(); err != nil {
		return err
	}
	i.detect()

	return nil
}

func (i *Info) tryLoadModFile() error {
	goModFile := url.Join(i.URL, "go.mod")
	fileContent, err := i.fs.DownloadWithURL(context.Background(), goModFile)
	if err != nil {
		return nil
	}
	i.Mod, err = modfile.Parse("go.mod", fileContent, nil)
	return err
}

func (i *Info) detect() {
	mod := i.Mod
	if mod != nil {
		return
	}

}

func NewInfo(ctx context.Context, URL string) (*Info, error) {
	var fs = afs.New()
	if err := ensureValidDirectory(ctx, fs, URL); err != nil {
		return nil, err
	}
	info := &Info{fs: fs, URL: URL}
	err := info.init(ctx)
	return info, err
}

func ensureValidDirectory(ctx context.Context, fs afs.Service, URL string) error {
	object, err := fs.Object(ctx, URL)
	if err != nil {
		return err
	}
	if !object.IsDir() {
		return fmt.Errorf("invalid URL: %v, expected folder location", URL)
	}
	return nil
}
