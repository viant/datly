package transcribe

import (
	"fmt"
	"sort"
)

// projectManifestEditor owns the complete mutation sequence for one project
// manifest update, including collision indexes and final dependency ordering.
type projectManifestEditor struct {
	manifest    *ProjectManifest
	routeOwners map[string]string
}

func newProjectManifestEditor(existing *ProjectManifest, prepared []preparedProjectComponent) (*projectManifestEditor, error) {
	manifest := existing.clone()
	if manifest == nil {
		manifest = &ProjectManifest{}
	}
	manifest.Version = projectManifestVersion
	editor := &projectManifestEditor{
		manifest: manifest, routeOwners: map[string]string{},
	}
	incoming := make(map[string]bool, len(prepared))
	for _, component := range prepared {
		incoming[component.identity] = true
	}
	for _, component := range manifest.Components {
		identity := component.Key.String()
		if incoming[identity] {
			continue
		}
		for _, route := range component.Routes {
			editor.routeOwners[projectRouteIdentity(route)] = identity
		}
	}
	for _, component := range prepared {
		for _, previous := range manifest.Components {
			if previous.Key.String() == component.identity && previous.Package != component.packagePath {
				return nil, fmt.Errorf("component %s package changed from %s to %s; explicit migration is required", component.identity, previous.Package, component.packagePath)
			}
		}
		if err := editor.reserve(component); err != nil {
			return nil, err
		}
	}
	return editor, nil
}

func (e *projectManifestEditor) reserve(component preparedProjectComponent) error {
	for _, route := range component.routes() {
		identity := projectRouteIdentity(route)
		if identity == "" {
			continue
		}
		if owner := e.routeOwners[identity]; owner != "" && owner != component.identity {
			return fmt.Errorf("route %q is shared by components %q and %q", identity, owner, component.identity)
		}
		e.routeOwners[identity] = component.identity
	}
	return nil
}

func (e *projectManifestEditor) Replace(component ProjectComponent, analysis ProjectMigrationComponent) {
	identity := component.Key.String()
	for index := range e.manifest.Components {
		if e.manifest.Components[index].Key.String() == identity {
			e.manifest.Components[index] = component
			e.manifest.Analysis.replace(analysis)
			return
		}
	}
	e.manifest.Components = append(e.manifest.Components, component)
	e.manifest.Analysis.replace(analysis)
}

func (e *projectManifestEditor) Finalize() *ProjectManifest {
	sort.Slice(e.manifest.Components, func(i, j int) bool {
		return e.manifest.Components[i].Key.String() < e.manifest.Components[j].Key.String()
	})
	sort.Slice(e.manifest.Analysis.Components, func(i, j int) bool {
		return e.manifest.Analysis.Components[i].Key.String() < e.manifest.Analysis.Components[j].Key.String()
	})
	seen := map[string]bool{}
	for _, component := range e.manifest.Components {
		for _, dependency := range component.Dependencies {
			seen[dependency] = true
		}
	}
	e.manifest.Dependencies = e.manifest.Dependencies[:0]
	for dependency := range seen {
		e.manifest.Dependencies = append(e.manifest.Dependencies, dependency)
	}
	sort.Strings(e.manifest.Dependencies)
	return e.manifest
}

func (r *ProjectMigrationReport) replace(component ProjectMigrationComponent) {
	identity := component.Key.String()
	for index := range r.Components {
		if r.Components[index].Key.String() == identity {
			r.Components[index] = component
			return
		}
	}
	r.Components = append(r.Components, component)
}

func (m *ProjectManifest) clone() *ProjectManifest {
	if m == nil {
		return nil
	}
	result := *m
	result.Components = append([]ProjectComponent(nil), m.Components...)
	for index := range result.Components {
		result.Components[index].Artifacts = append([]string(nil), m.Components[index].Artifacts...)
		result.Components[index].Routes = append([]ProjectRoute(nil), m.Components[index].Routes...)
		result.Components[index].Dependencies = append([]string(nil), m.Components[index].Dependencies...)
	}
	result.Dependencies = append([]string(nil), m.Dependencies...)
	result.Analysis.Components = append([]ProjectMigrationComponent(nil), m.Analysis.Components...)
	return &result
}
