package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func (p *scaffoldPersistence) retainResources(target, stage string, previous *scaffoldManifest, desired *[]string, roles map[string]string) (*ResourceManifest, error) {
	if p.plan == nil || p.plan.Resources == nil {
		if previous.Resources != nil {
			return nil, fmt.Errorf("generated package resources require explicit migration before disabling resource generation")
		}
		return nil, nil
	}
	resources := p.plan.Resources
	if previous.Resources != nil && previous.Resources.Namespace != resources.Namespace {
		return nil, fmt.Errorf("generated package resource namespace changed; explicit migration required")
	}
	result := &ResourceManifest{Namespace: resources.Namespace}
	seen := map[string]bool{}
	for _, file := range resources.Files {
		seen[file.Path] = true
		result.Files = append(result.Files, file.Path)
	}
	if previous.Resources != nil {
		for _, file := range previous.Resources.Files {
			if seen[file] || strings.HasPrefix(filepath.ToSlash(file), "datly_assets/static/") {
				continue
			}
			// Explicit overwrite retires obsolete generated resources. The
			// fingerprint guard still rejects removal of hand-edited bytes.
			if p.policy == GenerationPolicyOverwrite {
				continue
			}
			relative, err := managedRelativePath(file)
			if err != nil {
				return nil, err
			}
			if _, err = os.Stat(filepath.Join(stage, relative)); err != nil {
				return nil, fmt.Errorf("retained package resource %s: %w", relative, err)
			}
			result.Files = append(result.Files, relative)
			*desired = append(*desired, relative)
			roles[relative] = "artifact"
		}
	}
	sort.Strings(result.Files)
	sort.Strings(*desired)
	// The resource FS retains SQL referenced by append-only old shape files.
	emitted := *resources
	emitted.Files = nil
	for _, file := range result.Files {
		emitted.Files = append(emitted.Files, EmittedFile{Path: file})
	}
	for i := range p.files {
		relative, err := managedPath(target, p.files[i].Path)
		if err != nil {
			return nil, err
		}
		if relative == resources.Destination {
			p.files[i].Content = emitted.source(p.plan.PackageName())
			break
		}
	}
	return result, nil
}
