package transcribe

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/transcribe/internal/dependency"
	xmodule "github.com/viant/x/module"
)

// BatchRule selects a source file, local directory, or module-qualified package
// pattern. Directory selections recurse. Operation optionally overrides the
// request generator, allowing an ordered writer/reader dependency chain.
type BatchRule struct{ Source, Operation string }

type BatchRequest struct {
	Generator   Generator
	Destination string
	// Rules are processed in caller order; each selection is sorted by source
	// path. When empty, Discovery.Include supplies the selections.
	Rules []BatchRule
}

type BatchResult struct {
	Sources  []string
	Packages []*GeneratedPackage
	// DependencyParses and DependencyReuses count dependency package AST loads,
	// not all Go parsing or build commands performed during generation.
	DependencyParses, DependencyReuses int
}

type batchSource struct {
	file      xmodule.File
	operation string
}

// TranscribeBatch discovers sources once, then compiles and publishes each rule
// before compiling the next. Exclude and module .goignore retain workspace
// discovery semantics. On failure it returns completed results and a source-
// attributed error; earlier publications are not rolled back.
func (d *Discovery) TranscribeBatch(ctx context.Context, request BatchRequest) (*BatchResult, error) {
	if d == nil {
		return nil, fmt.Errorf("transcribe discovery is required")
	}
	staged := *d
	workspace := staged.Workspace
	var err error
	if workspace == nil {
		workspace, err = (xmodule.LocalWorkspace{BaseDir: d.BaseDir, ModuleDirs: d.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	staged.Workspace = workspace
	rules := request.Rules
	if len(rules) == 0 {
		for _, include := range d.Include {
			rules = append(rules, BatchRule{Source: include})
		}
	}
	type selection struct{ source, pattern, file, operation string }
	var selections []selection
	var patterns []string
	for _, rule := range rules {
		operation := rule.Operation
		if operation == "" {
			operation = request.Generator.Operation
		}
		switch operation {
		case "get", "patch", "post", "put", "handler":
		default:
			return nil, fmt.Errorf("invalid batch operation %q for %s", operation, rule.Source)
		}
		pattern, selected, err := batchPattern(d.BaseDir, rule.Source)
		if err != nil {
			return nil, err
		}
		selections = append(selections, selection{source: rule.Source, pattern: pattern, file: selected, operation: operation})
		patterns = append(patterns, pattern)
	}
	if len(patterns) == 0 {
		return nil, fmt.Errorf("batch selection contains no component sources")
	}
	inventory, err := discoverFiles(ctx, workspace, patterns, d.Exclude)
	if err != nil {
		return nil, err
	}
	var sources []batchSource
	seen := map[string]string{}
	for _, selection := range selections {
		pattern, selected, operation := selection.pattern, selection.file, selection.operation
		var files []xmodule.File
		candidates, err := inventory.selectFiles(ctx, []string{pattern}, d.Exclude)
		if err != nil {
			return nil, fmt.Errorf("select batch source %q: %w", selection.source, err)
		}
		for _, file := range candidates {
			if !sourceFileExtensions[strings.ToLower(filepath.Ext(file.Path))] {
				continue
			}
			if selected != "" && filepath.Clean(file.Path) != selected {
				continue
			}
			if d.SourceFile != "" && filepath.Base(file.Path) != d.SourceFile {
				continue
			}
			files = append(files, file)
		}
		if selected != "" && len(files) == 0 {
			return nil, fmt.Errorf("batch source %q is excluded or unavailable", selection.source)
		}
		sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
		for _, file := range files {
			if previous, ok := seen[file.Path]; ok {
				if previous != operation {
					return nil, fmt.Errorf("batch source %q selected with conflicting operations", file.Path)
				}
				continue
			}
			seen[file.Path] = operation
			sources = append(sources, batchSource{file: file, operation: operation})
		}
	}
	if d.SourceName != "" && len(sources) != 1 {
		return nil, fmt.Errorf("source-name requires exactly one selected source file; found %d", len(sources))
	}
	result := &BatchResult{}
	session := &dependency.Session{}
	ctx = dependency.WithSession(ctx, session)
	defer func() { result.DependencyParses, result.DependencyReuses = session.Parses, session.Hits }()
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		selected := staged
		selected.Include = []string{source.file.ImportPath}
		selected.SourceFile = filepath.Base(source.file.Path)
		project, err := selected.Compile(ctx)
		if err != nil {
			return result, fmt.Errorf("transcribe batch source %q: %w", source.file.Path, err)
		}
		if len(project.Components) == 0 {
			continue
		} // route-free SQL remains a resource
		if len(project.Components) != 1 {
			return result, fmt.Errorf("batch source %q requires exactly one component; found %d", source.file.Path, len(project.Components))
		}
		generator := request.Generator
		generator.Operation = source.operation
		generated, err := generator.Generate(ctx, GenerationRequest{Compiled: project.Components[0], Destination: request.Destination})
		if err != nil {
			return result, fmt.Errorf("transcribe batch source %q: %w", source.file.Path, err)
		}
		result.Sources = append(result.Sources, source.file.Path)
		result.Packages = append(result.Packages, generated)
	}
	if len(result.Packages) == 0 {
		return result, fmt.Errorf("batch selection contains no component sources")
	}
	return result, nil
}

func batchPattern(base, selection string) (pattern, file string, err error) {
	if strings.TrimSpace(selection) == "" {
		return "", "", fmt.Errorf("empty batch source selection")
	}
	local := selection
	if !filepath.IsAbs(local) {
		local = filepath.Join(base, local)
	}
	info, statErr := os.Stat(local)
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			return "", "", statErr
		}
		if filepath.IsAbs(selection) || strings.HasPrefix(selection, ".") || sourceFileExtensions[strings.ToLower(filepath.Ext(selection))] {
			return "", "", fmt.Errorf("batch source %q: %w", selection, statErr)
		}
		return selection, "", nil
	}
	local, err = filepath.EvalSymlinks(local)
	if err != nil {
		return "", "", err
	}
	local, err = filepath.Abs(local)
	if err != nil {
		return "", "", err
	}
	directory := local
	if !info.IsDir() {
		directory, file = filepath.Dir(local), local
	}
	module, err := xmodule.LocateLocal(directory)
	if err != nil {
		return "", "", err
	}
	pattern, err = xmodule.ImportPathLocal(module.Dir, module.Path, directory)
	if err != nil {
		return "", "", err
	}
	if info.IsDir() {
		pattern += "/..."
	}
	return pattern, file, nil
}
