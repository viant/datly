package generate

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/viant/datly/typecatalog"
)

// Linked contracts keep their existing SQL URIs and embedded filesystem. A
// Go-only reload does not disable resource generation or recreate those files.
func (r *planResolver) linkedResources(plan *ResourcePlan) (*ResourcePlan, error) {
	if !r.input.SQLResources || r.input.ProjectRoot == "" ||
		r.plan.Input.Ownership != ContractLinked || r.plan.Output.Ownership != ContractLinked {
		return nil, nil
	}
	for _, contract := range []ContractPlan{r.plan.Input, r.plan.Output} {
		descriptor, err := r.types.Descriptor(contract.DescriptorKey)
		if err != nil {
			return nil, err
		}
		if descriptor == nil || descriptor.PkgPath != r.input.TargetPackage {
			return nil, nil
		}
	}
	for _, view := range r.plan.Views {
		if view.Ownership != ViewLinked {
			return nil, nil
		}
	}
	authority, err := typecatalog.NewDestinationAuthority(r.input.ProjectRoot)
	if err != nil {
		return nil, err
	}
	destination, err := authority.Package(r.input.TargetPackage, "")
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(r.input.ProjectRoot, destination.Directory)
	manifest, err := readScaffoldManifest(dir)
	if err != nil {
		return nil, err
	}
	manifest, err = manifest.forOwner(r.plan.ComponentName, true)
	if err != nil {
		return nil, err
	}
	if !manifest.exists || manifest.Resources == nil {
		return nil, nil
	}
	if manifest.Identity != r.plan.OwnerIdentity || manifest.ComponentPackage != r.input.TargetPackage || manifest.Resources.Namespace != plan.Namespace {
		return nil, fmt.Errorf("linked package resource ownership changed; explicit migration required")
	}
	if err = manifest.Resources.Validate(); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	content, err := fs.ReadFile(root.FS(), plan.Destination)
	if err != nil {
		return nil, err
	}
	plan.sourceText = string(content)
	for _, file := range manifest.Resources.Files {
		content, err = fs.ReadFile(root.FS(), file)
		if err != nil {
			return nil, err
		}
		plan.Files = append(plan.Files, EmittedFile{Path: file, Content: string(content)})
	}
	return plan, nil
}
