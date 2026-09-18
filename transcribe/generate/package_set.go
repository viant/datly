package generate

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/typecatalog"
	xmodule "github.com/viant/x/module"
	xshape "github.com/viant/x/shape"
)

// packageSet preflights all authored destinations and the import graph before
// handing each package to the existing transactional persistence owner.
type packageSet struct {
	plans    []*Plan
	dirs     []string
	files    [][]EmittedFile
	removals []map[string]bool
}

func (p *Plan) packages(dir string) (*packageSet, error) {
	result := &packageSet{plans: []*Plan{p}, dirs: []string{dir}}
	if len(p.ShapePackages) > 0 {
		authority, err := typecatalog.NewDestinationAuthority(p.ProjectRoot)
		if err != nil {
			return nil, err
		}
		for _, plan := range p.ShapePackages {
			dest, err := authority.Package(plan.Package, "")
			if err != nil {
				return nil, err
			}
			result.plans = append(result.plans, plan)
			result.dirs = append(result.dirs, filepath.Join(p.ProjectRoot, dest.Directory))
		}
	}
	for i, plan := range result.plans {
		files, _, _, err := scaffoldArtifacts(result.dirs[i], plan)
		if err != nil {
			return nil, err
		}
		result.files = append(result.files, files)
	}
	return result, nil
}
func (s *packageSet) validate() error {
	s.removals = make([]map[string]bool, len(s.plans))
	for i, p := range s.plans {
		files, user, removals, err := scaffoldArtifacts(s.dirs[i], p)
		if err != nil {
			return err
		}
		persistence := &scaffoldPersistence{dir: s.dirs[i], owner: p.ComponentName, files: files, userFiles: user, removals: removals, plan: p}
		preview, err := persistence.preview()
		if err != nil {
			return err
		}
		s.files[i] = preview.files
		s.removals[i] = preview.renames
		for _, file := range preview.userFiles {
			if _, err := os.Stat(file.Path); os.IsNotExist(err) {
				s.files[i] = append(s.files[i], file)
			} else if err != nil {
				return err
			}
		}
		if p.ProjectRoot != "" {
			if err = s.validatePackage(i); err != nil {
				return err
			}
		}
	}
	return s.validateImports()
}

// validateEphemeral avoids the persistence preview and its ownership checks.
// Existing sidecar-free generated resources are not user-owned files.
func (s *packageSet) validateEphemeral() error {
	for i, p := range s.plans {
		files, _, _, err := scaffoldArtifacts(s.dirs[i], p)
		if err != nil {
			return err
		}
		s.files[i] = files
		if p.ProjectRoot != "" {
			if err = s.validatePackage(i); err != nil {
				return err
			}
		}
	}
	return s.validateImports()
}
func (s *packageSet) sources(index int) (map[string]string, error) {
	sources := map[string]string{}
	entries, err := os.ReadDir(s.dirs[index])
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(s.dirs[index], entry.Name()))
		if err != nil {
			return nil, err
		}
		sources[entry.Name()] = string(content)
	}
	for _, group := range s.files {
		for _, file := range group {
			if filepath.Clean(filepath.Dir(file.Path)) != filepath.Clean(s.dirs[index]) {
				continue
			}
			if strings.HasSuffix(file.Path, ".go") {
				sources[filepath.Base(file.Path)] = file.Content
			}
		}
	}
	manifest, err := readScaffoldManifest(s.dirs[index])
	if err != nil {
		return nil, err
	}
	manifest, err = manifest.forOwner(s.plans[index].ComponentName, s.plans[index].GoPackage != "")
	if err != nil {
		return nil, err
	}
	for _, name := range manifest.Files {
		if manifest.Roles[name] != "artifact" && (index >= len(s.removals) || !s.removals[index][name]) {
			continue
		}
		proposed := false
		for _, group := range s.files {
			for _, file := range group {
				if filepath.Clean(file.Path) == filepath.Clean(filepath.Join(s.dirs[index], name)) {
					proposed = true
				}
			}
		}
		if !proposed {
			delete(sources, name)
		}
	}
	return sources, nil
}
func (s *packageSet) validatePackage(index int) error {
	sources, err := s.sources(index)
	if err != nil {
		return err
	}
	owners := map[string]string{}
	names := make([]string, 0, len(sources))
	for name := range sources {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parsed, err := (xshape.SourceParser{}).Parse([]byte(sources[name]))
		if err != nil {
			return err
		}
		if parsed.Package != s.plans[index].PackageName() {
			return fmt.Errorf("destination %s has conflicting package clauses %s and %s", s.dirs[index], parsed.Package, s.plans[index].PackageName())
		}
		for _, symbol := range parsed.Declarations {
			if previous := owners[symbol]; previous != "" {
				return fmt.Errorf("destination %s declaration %s collides in %s and %s", s.dirs[index], symbol, previous, name)
			}
			owners[symbol] = name
		}
	}
	return nil
}
func (s *packageSet) validateImports() error {
	root := s.plans[0].ProjectRoot
	if root == "" {
		return nil
	}
	module, err := xmodule.LocateLocal(root)
	if err != nil {
		return err
	}
	graph := map[string]map[string]bool{}
	proposed := map[string]map[string]string{}
	for i, p := range s.plans {
		sources, err := s.sources(i)
		if err != nil {
			return err
		}
		proposed[filepath.Clean(s.dirs[i])] = sources
		graph[p.Package] = map[string]bool{}
	}
	// SourceParser owns Go imports. Include existing project packages so a shape
	// cannot introduce a cycle through an authored intermediary.
	err = filepath.WalkDir(root, func(file string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		if file != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor") {
			return filepath.SkipDir
		}
		if file != root {
			if _, err := os.Stat(filepath.Join(file, "go.mod")); err == nil {
				return filepath.SkipDir
			}
		}
		if content, err := os.ReadFile(filepath.Join(file, scaffoldManifestName)); err == nil {
			manifest := &scaffoldManifest{}
			if err = json.Unmarshal(content, manifest); err != nil {
				return err
			}
			if err = manifest.validateComponentDestination(s.plans); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		if _, ok := proposed[filepath.Clean(file)]; ok {
			return nil
		}
		files, err := os.ReadDir(file)
		if err != nil {
			return err
		}
		hasGoSource := false
		for _, item := range files {
			if !item.IsDir() && strings.HasSuffix(item.Name(), ".go") && !strings.HasSuffix(item.Name(), "_test.go") {
				hasGoSource = true
				break
			}
		}
		if !hasGoSource {
			return nil
		}
		importPath, err := xmodule.ImportPathLocal(module.Dir, module.Path, file)
		if err != nil {
			return err
		}
		for _, item := range files {
			if item.IsDir() || !strings.HasSuffix(item.Name(), ".go") || strings.HasSuffix(item.Name(), "_test.go") {
				continue
			}
			content, err := os.ReadFile(filepath.Join(file, item.Name()))
			if err != nil {
				return err
			}
			if err = s.imports(graph, importPath, string(content)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for i, p := range s.plans {
		for _, content := range proposed[filepath.Clean(s.dirs[i])] {
			if err = s.imports(graph, p.Package, content); err != nil {
				return err
			}
		}
	}
	active, done := map[string]bool{}, map[string]bool{}
	var visit func(string, []string) error
	visit = func(pkg string, chain []string) error {
		if active[pkg] {
			return fmt.Errorf("generation import cycle: %s", strings.Join(append(chain, pkg), " -> "))
		}
		if done[pkg] {
			return nil
		}
		active[pkg] = true
		for child := range graph[pkg] {
			if err := visit(child, append(chain, pkg)); err != nil {
				return err
			}
		}
		delete(active, pkg)
		done[pkg] = true
		return nil
	}
	for pkg := range graph {
		if err := visit(pkg, nil); err != nil {
			return err
		}
	}
	return nil
}
func (s *packageSet) imports(graph map[string]map[string]bool, pkg, source string) error {
	parsed, err := (xshape.SourceParser{}).Parse([]byte(source))
	if err != nil {
		return err
	}
	if graph[pkg] == nil {
		graph[pkg] = map[string]bool{}
	}
	for _, imp := range parsed.Imports {
		graph[pkg][imp.Path] = true
	}
	return nil
}

// PackagePlan is a planned component and its component directory.
type PackagePlan struct {
	Plan      *Plan
	Directory string
}
type Packages []PackagePlan

func (p Packages) Validate() error {
	all := &packageSet{}
	paths := map[string]string{}
	for _, entry := range p {
		group, err := entry.Plan.packages(entry.Directory)
		if err != nil {
			return err
		}
		for _, files := range group.files {
			for _, file := range files {
				key := filepath.Clean(file.Path)
				if prior := paths[key]; prior != "" {
					return fmt.Errorf("generated destination %s collides between components %s and %s", key, prior, entry.Plan.ComponentName)
				}
				paths[key] = entry.Plan.ComponentName
			}
		}
		all.plans = append(all.plans, group.plans...)
		all.dirs = append(all.dirs, group.dirs...)
		all.files = append(all.files, group.files...)
	}
	return all.validate()
}

// ValidateEphemeral validates the generated package layout without treating
// existing generated artifacts as user-owned merely because no persistence
// sidecar is present. It is deliberately limited to read-only validation.
func (p Packages) ValidateEphemeral() error {
	all := &packageSet{}
	paths := map[string]string{}
	for _, entry := range p {
		group, err := entry.Plan.packages(entry.Directory)
		if err != nil {
			return err
		}
		for _, files := range group.files {
			for _, file := range files {
				key := filepath.Clean(file.Path)
				if prior := paths[key]; prior != "" {
					return fmt.Errorf("generated destination %s collides between components %s and %s", key, prior, entry.Plan.ComponentName)
				}
				paths[key] = entry.Plan.ComponentName
			}
		}
		all.plans = append(all.plans, group.plans...)
		all.dirs = append(all.dirs, group.dirs...)
		all.files = append(all.files, group.files...)
	}
	return all.validateEphemeral()
}
