package generate

import (
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
	plans     []*Plan
	dirs      []string
	files     [][]EmittedFile
	removals  []map[string]bool
	templates []*scaffoldPersistence
	previews  []*scaffoldPersistence
	readRoots []string
	forests   []*scaffoldForest
}

func (p *Plan) packages(dir string) (*packageSet, error) {
	result, err := p.packageTargets(dir)
	if err != nil {
		return nil, err
	}
	if err := result.render(); err != nil {
		return nil, err
	}
	return result, nil
}

// packageTargets resolves destinations without inspecting their current files.
func (p *Plan) packageTargets(dir string) (*packageSet, error) {
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
	return result, nil
}

func (s *packageSet) render() error {
	for i, plan := range s.plans {
		files, _, _, err := scaffoldArtifacts(s.dirs[i], plan)
		if err != nil {
			return err
		}
		s.files = append(s.files, files)
	}
	return nil
}

// lockTargets holds destination ownership across preview, validation and
// persistence. A preview must never see another writer's directory swap gap.
func (s *packageSet) lockTargets() (func(), error) {
	targets := make(map[string]bool, len(s.dirs))
	for i, dir := range s.dirs {
		target, err := (&scaffoldPersistence{dir: dir, owner: s.plans[i].ComponentName}).target()
		if err != nil {
			return nil, err
		}
		targets[target] = true
	}
	ordered := make([]string, 0, len(targets))
	for target := range targets {
		ordered = append(ordered, target)
	}
	sort.Strings(ordered)
	releases := make([]func(), 0, len(ordered))
	for _, target := range ordered {
		releases = append(releases, scaffoldLocks.acquire(target))
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}, nil
}

func (s *packageSet) validate() error {
	s.removals = make([]map[string]bool, len(s.plans))
	s.templates = make([]*scaffoldPersistence, len(s.plans))
	s.previews = make([]*scaffoldPersistence, len(s.plans))
	for i, p := range s.plans {
		files, user, removals, err := scaffoldArtifacts(s.dirs[i], p)
		if err != nil {
			return err
		}
		persistence := &scaffoldPersistence{dir: s.dirs[i], owner: p.ComponentName, files: files, userFiles: user, removals: removals, plan: p}
		s.templates[i] = persistence.detached()
		preview, err := persistence.preview()
		if err != nil {
			return err
		}
		s.previews[i] = preview
		s.files[i] = preview.files
		s.removals[i] = preview.renames
		for _, file := range preview.userFiles {
			if _, err := os.Stat(file.Path); os.IsNotExist(err) {
				s.files[i] = append(s.files[i], file)
			} else if err != nil {
				return err
			}
		}
		if p.ExternalHandler != nil && p.ExternalHandler.Build != nil {
			if err = s.validateSourceHandler(i); err != nil {
				return err
			}
		} else if p.ProjectRoot != "" {
			if err = s.validatePackage(i); err != nil {
				return err
			}
		}
	}
	if len(s.plans) == 1 && s.plans[0].ExternalHandler != nil && s.plans[0].ExternalHandler.Build != nil {
		return nil // Go's build-selected graph, not an unfiltered AST graph, is authoritative.
	}
	return s.validateImports()
}

func (s *packageSet) validateSourceHandler(index int) error {
	sources, err := s.sources(index)
	if err != nil {
		return err
	}
	files := map[string][]byte{}
	entries, err := os.ReadDir(s.dirs[index])
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			files[filepath.Join(s.dirs[index], entry.Name())] = nil
		}
	}
	for name, content := range sources {
		files[filepath.Join(s.dirs[index], name)] = []byte(content)
	}
	// Factory validation must see the projected resource files referenced by
	// generated go:embed declarations, as well as the selected Go sources.
	for _, file := range s.files[index] {
		rel, err := filepath.Rel(s.dirs[index], file.Path)
		if err != nil {
			return err
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !strings.HasSuffix(file.Path, ".go") {
			files[file.Path] = []byte(file.Content)
		}
	}
	return s.plans[index].ExternalHandler.Build.Validate(s.plans[index].Package, files)
}

func (s *packageSet) sources(index int) (map[string]string, error) {
	sources := map[string]string{}
	readRoot := s.dirs[index]
	if len(s.readRoots) > index {
		readRoot = s.readRoots[index]
	}
	entries, err := os.ReadDir(readRoot)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		content, err := os.ReadFile(filepath.Join(readRoot, entry.Name()))
		if err != nil {
			return nil, err
		}
		sources[entry.Name()] = string(content)
	}
	if len(s.readRoots) > index {
		return sources, nil
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
	if index < len(s.removals) {
		for name := range s.removals[index] {
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
	walk := func(readRoot, nominalRoot string, physical bool) error {
		return filepath.WalkDir(readRoot, func(file string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if physical {
				for _, forest := range s.forests {
					if filepath.Clean(file) == forest.target {
						return filepath.SkipDir
					}
				}
			}
			if !entry.IsDir() {
				return nil
			}
			if file != readRoot && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			if file != readRoot {
				if _, err := os.Stat(filepath.Join(file, "go.mod")); err == nil {
					return filepath.SkipDir
				}
			}
			relative, err := filepath.Rel(readRoot, file)
			if err != nil {
				return err
			}
			nominal := filepath.Join(nominalRoot, relative)
			if _, ok := proposed[filepath.Clean(nominal)]; ok {
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
			importPath, err := xmodule.ImportPathLocal(module.Dir, module.Path, nominal)
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
	}
	err = walk(root, root, true)
	if err == nil {
		for _, forest := range s.forests {
			if err = walk(forest.stage, forest.target, false); err != nil {
				break
			}
		}
	}
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

func (s *packageSet) validateProjected() error {
	for i, p := range s.plans {
		if p.ExternalHandler != nil && p.ExternalHandler.Build != nil {
			persistence := s.templates[i].detached()
			target, err := persistence.target()
			if err != nil {
				return err
			}
			if err = persistence.validateHandlerStage(target, s.readRoots[i]); err != nil {
				return err
			}
		} else if p.ProjectRoot != "" {
			if err := s.validatePackage(i); err != nil {
				return err
			}
		}
	}
	if len(s.plans) == 1 && s.plans[0].ExternalHandler != nil && s.plans[0].ExternalHandler.Build != nil {
		return nil
	}
	return s.validateImports()
}
