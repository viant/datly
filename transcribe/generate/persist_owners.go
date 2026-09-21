package generate

import (
	"fmt"
	"github.com/viant/datly/typecatalog"
	smodel "github.com/viant/x/syntetic/model"
	"strings"
)

// forOwner selects only the current component's regeneration evidence. The
// aggregate keeps every other owner's manifest intact during package staging.
func (m *scaffoldManifest) forOwner(owner string, shared bool) (*scaffoldManifest, error) {
	if m.Owners == nil {
		if !m.exists || m.Owner == owner {
			return m, nil
		}
		if !shared {
			return nil, fmt.Errorf("generated package is owned by component %q, not %q", m.Owner, owner)
		}
		m = &scaffoldManifest{Owners: map[string]*scaffoldManifest{m.Owner: m}, exists: true}
	}
	result := &scaffoldManifest{Owner: owner, others: map[string]*scaffoldManifest{}}
	seen := map[string]string{}
	for key, member := range m.Owners {
		if member == nil || key == "" || key != member.Owner || member.Owners != nil {
			return nil, fmt.Errorf("invalid component ownership manifest %q", key)
		}
		for _, file := range member.Files {
			if prior := seen[file]; prior != "" && prior != key {
				return nil, fmt.Errorf("file %q is owned by both %s and %s", file, prior, key)
			}
			seen[file] = key
		}
		if key == owner {
			copy := *member
			result = &copy
			result.exists = true
			result.others = map[string]*scaffoldManifest{}
		}
	}
	for key, member := range m.Owners {
		if key != owner {
			result.others[key] = member
		}
	}
	return result, nil
}

func (m *scaffoldManifest) aggregate() *scaffoldManifest {
	if len(m.others) == 0 {
		return m
	}
	result := &scaffoldManifest{Version: scaffoldManifestVersion, Owners: map[string]*scaffoldManifest{}}
	for key, member := range m.others {
		result.Owners[key] = member
	}
	result.Owners[m.Owner] = m
	return result
}

func (m *scaffoldManifest) foreignFiles() map[string]string {
	result := map[string]string{}
	for owner, member := range m.others {
		for _, file := range member.Files {
			result[file] = owner
		}
	}
	return result
}

func (p *scaffoldPersistence) validateForeignFiles(manifest *scaffoldManifest) error {
	foreign := manifest.foreignFiles()
	files, err := p.generatedPaths()
	if err != nil {
		return err
	}
	for _, file := range append(files, p.removals...) {
		for existing, owner := range foreign {
			if file == existing || strings.HasPrefix(file, existing+"/") || strings.HasPrefix(existing, file+"/") {
				return fmt.Errorf("generated destination %q conflicts with component %q file %q", file, owner, existing)
			}
		}
	}
	return nil
}

// RegisterPackage publishes generated declarations under manifest ownership and
// preserves authored declarations that share the same Go package.
func (r *Result) RegisterPackage(catalog *typecatalog.Catalog, pkg *smodel.Package, dir string) error {
	manifest, err := readScaffoldMetadata(dir)
	if err != nil {
		return err
	}
	if manifest.exists && manifest.Version == 0 && manifest.isResourceOnly() {
		return catalog.RegisterPackageFiles(pkg, nil)
	}
	if manifest.exists {
		if err = manifest.validateVersion(); err != nil {
			return err
		}
	}
	files := map[string]bool{}
	for _, file := range manifest.Files {
		files[file] = true
	}
	for _, member := range manifest.Owners {
		for _, file := range member.Files {
			files[file] = true
		}
	}
	return catalog.RegisterPackageFiles(pkg, files)
}

func (m *scaffoldManifest) isResourceOnly() bool {
	return m != nil && m.Resources != nil && m.Owner == "" && m.Identity == "" && m.ComponentPackage == "" && len(m.Files) == 0 && len(m.Owners) == 0
}

func (p *scaffoldPersistence) destinationMetadata() *scaffoldManifest {
	if p.plan == nil {
		return &scaffoldManifest{}
	}
	return &scaffoldManifest{Identity: p.plan.OwnerIdentity, ComponentPackage: p.plan.ComponentPackage, Destinations: p.plan.Destinations}
}

func (m *scaffoldManifest) validateComponentDestination(plans []*Plan) error {
	for _, p := range plans {
		if m.Identity != "" && m.Identity == p.OwnerIdentity && m.ComponentPackage != "" && m.ComponentPackage != p.ComponentPackage {
			return fmt.Errorf("component %s destination changed from %s to %s; explicit migration is required", m.Identity, m.ComponentPackage, p.ComponentPackage)
		}
	}
	for _, member := range m.Owners {
		if member == nil {
			return fmt.Errorf("invalid nil component ownership")
		}
		if err := member.validateComponentDestination(plans); err != nil {
			return err
		}
	}
	return nil
}
