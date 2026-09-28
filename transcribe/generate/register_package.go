package generate

import (
	"os"
	"path/filepath"

	"github.com/viant/datly/typecatalog"
	smodel "github.com/viant/x/syntetic/model"
)

// RegisterPackage classifies current emitted artifacts and generated source.
// Application hooks and explicit linked contracts retain package authority.
func (r *Result) RegisterPackage(catalog *typecatalog.Catalog, pkg *smodel.Package, dir string) error {
	files := map[string]bool{}
	for _, file := range pkg.Files {
		content, err := os.ReadFile(filepath.Join(dir, filepath.Base(file.Name)))
		if err != nil {
			return err
		}
		if generatedOwner(content) != "" {
			files[filepath.Base(file.Name)] = true
		}
	}
	if r != nil {
		for _, file := range r.Files {
			if filepath.Clean(filepath.Dir(file.Path)) == filepath.Clean(dir) {
				files[filepath.Base(file.Path)] = true
			}
		}
	}
	return catalog.RegisterPackageFiles(pkg, files)
}
