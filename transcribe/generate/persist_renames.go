package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	xshape "github.com/viant/x/shape"
)

// prepareRenames identifies replaced shapes only among this manifest owner's
// files. Native declaration parsing proves that every old declaration has a
// new destination. protectArtifacts still requires unchanged trusted bytes
// before removing any old file; authored files never enter this candidate set.
func (p *scaffoldPersistence) prepareRenames(existing string, manifest *scaffoldManifest) error {
	p.renames = map[string]bool{}
	if p.plan == nil {
		return nil
	}
	for role, previous := range manifest.Destinations {
		next := p.plan.Destinations[role]
		if next == "" || next == previous {
			continue
		}
		oldPath, oldType, oldOK := strings.Cut(previous, ":")
		nextPath, nextType, nextOK := strings.Cut(next, ":")
		if !oldOK || !nextOK || oldType != nextType || filepath.Dir(oldPath) != filepath.Dir(nextPath) {
			return fmt.Errorf("shape %s destination changed from %s to %s; explicit migration is required", role, previous, next)
		}
	}
	target, err := p.target()
	if err != nil {
		return err
	}
	shapes := p.shapeDestinations()
	declarations := map[string]string{}
	for _, file := range p.files {
		relative, err := managedPath(target, file.Path)
		if err != nil {
			return err
		}
		if !shapes[relative] {
			continue
		}
		parsed, err := (xshape.SourceParser{}).Parse([]byte(file.Content))
		if err != nil {
			return err
		}
		for _, name := range parsed.Declarations {
			declarations[name] = relative
		}
	}
	for _, file := range manifest.Files {
		if shapes[file] || manifest.Roles[file] != "shape" {
			continue
		}
		content, err := os.ReadFile(filepath.Join(existing, file))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		parsed, err := (xshape.SourceParser{}).Parse(content)
		if err != nil {
			return err
		}
		replaced := 0
		for _, name := range parsed.Declarations {
			if declarations[name] != "" {
				replaced++
			}
		}
		if replaced == 0 {
			continue
		}
		if replaced != len(parsed.Declarations) {
			return fmt.Errorf("generated shape %q has declarations without new destinations: explicit migration required", file)
		}
		p.renames[file] = true
	}
	return nil
}
