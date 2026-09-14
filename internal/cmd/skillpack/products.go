package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"
)

type productDeclaration struct {
	Files       []string
	Directories map[string][]string
	Exports     map[string]struct{ StartAt, EndBefore, Reason string }
}

// Product imports are exact, operator-reviewed files. There is no recursive
// source-folder copy and no automatic expansion of the approved import set.
func (p packager) products(contents map[string][]byte) (map[string]string, error) {
	manifest, err := os.ReadFile(path.Join(p.source, "packaging.json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var declaration productDeclaration
	decoder := json.NewDecoder(bytes.NewReader(manifest))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&declaration); err != nil {
		return nil, err
	}
	sources := os.DirFS(p.productsRoot)
	targets := map[string]string{}
	payloads := map[string][]byte{}
	provenance := map[string]string{}
	for _, ref := range declaration.Files {
		if !fs.ValidPath(ref) || strings.Contains(ref, "\\") || targets[ref] != "" {
			return nil, fmt.Errorf("invalid or duplicate product import %q", ref)
		}
		if !strings.HasPrefix(ref, "datly/") && !strings.HasPrefix(ref, "xdatly/") && !strings.HasPrefix(ref, "llm/") {
			return nil, fmt.Errorf("unapproved product root %q", ref)
		}
		// Validate every ancestor before reading; never follow source symlinks.
		source := fs.FS(sources)
		sourceRef := ref
		if strings.HasPrefix(ref, "llm/") {
			source = os.DirFS(p.source)
			sourceRef = strings.TrimPrefix(ref, "llm/")
		}
		current := "."
		for _, part := range strings.Split(sourceRef, "/") {
			entries, err := fs.ReadDir(source, current)
			if err != nil {
				return nil, err
			}
			found := false
			for _, entry := range entries {
				if entry.Name() == part {
					found = true
					if entry.Type()&fs.ModeSymlink != 0 {
						return nil, fmt.Errorf("product symlink %s", ref)
					}
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("missing product source %s", ref)
			}
			current = path.Join(current, part)
		}
		info, err := fs.Stat(source, sourceRef)
		if err != nil {
			return nil, err
		}
		if !info.Mode().IsRegular() || info.Size() > 4<<20 {
			return nil, fmt.Errorf("invalid product file %s", ref)
		}
		data, err := fs.ReadFile(source, sourceRef)
		if err != nil {
			return nil, err
		}
		provenance[ref] = fmt.Sprintf("%x", sha256.Sum256(data))
		if profile, ok := declaration.Exports[ref]; ok {
			if profile.StartAt != "" {
				start := bytes.Index(data, []byte(profile.StartAt))
				if start < 0 {
					return nil, fmt.Errorf("invalid public export start %s", ref)
				}
				data = data[start:]
			}
			boundary := strings.Index(string(data), profile.EndBefore)
			if boundary < 0 || profile.EndBefore == "" || profile.Reason == "" {
				return nil, fmt.Errorf("invalid public export boundary %s", ref)
			}
			data = append(append([]byte(nil), data[:boundary]...), []byte("\n> Packaging boundary: "+profile.Reason+"\n")...)
		}
		targets[ref] = "references/product/" + ref
		if strings.HasPrefix(ref, "llm/") && path.Base(ref) == "SKILL.md" {
			targets[ref] = strings.TrimSuffix(targets[ref], "SKILL.md") + "SKILL.reference.md"
		}
		if strings.HasSuffix(ref, ".go") || path.Base(ref) == "go.mod" {
			targets[ref] += ".txt"
		}
		payloads[ref] = data
	}
	for directory, children := range declaration.Directories {
		if !fs.ValidPath(directory) || targets[directory] != "" {
			return nil, fmt.Errorf("invalid directory reference %s", directory)
		}
		text := "# Published source reference index\n\nOnly the explicitly approved files below are included. This is not a full source-tree export.\n\n"
		for _, child := range children {
			if targets[child] == "" {
				return nil, fmt.Errorf("unapproved index child %s", child)
			}
			text += "- [" + child + "](" + targets[child] + ")\n"
		}
		targets[directory] = "references/product/" + directory + "/INDEX.md"
		payloads[directory] = []byte(text)
	}
	var missing []string
	for _, skill := range []string{"datly-reader", "datly-writer", "datly-custom-component"} {
		for ref, data := range payloads {
			output := data
			if strings.HasSuffix(ref, ".md") {
				output, err = rewriteLinks(data, func(raw string) (string, error) {
					u, err := localReference(raw)
					if err != nil || u == nil {
						return raw, err
					}
					sourceRef := ref
					if u.Path != "" {
						sourceRef = path.Clean(path.Join(path.Dir(ref), u.Path))
						if strings.HasPrefix(ref, "llm/") {
							parts := strings.SplitN(ref, "/", 3)
							sourceRef = path.Join("llm", parts[1], u.Path)
							if strings.HasPrefix(u.Path, "references/product/") {
								sourceRef = strings.TrimPrefix(u.Path, "references/product/")
							}
						}
					}
					// Product guides link to the repository's distributed source
					// tree. Canonical imports keep the logical llm/ authority and
					// are always read from the explicitly selected -source root.
					if strings.HasPrefix(sourceRef, "datly/llm/") {
						sourceRef = strings.TrimPrefix(sourceRef, "datly/")
					}
					target := targets[sourceRef]
					if strings.HasPrefix(sourceRef, "llm/"+skill+"/") {
						target = strings.TrimPrefix(sourceRef, "llm/"+skill+"/")
					}
					if target == "" {
						missing = append(missing, ref+" -> "+raw+" ["+sourceRef+"]")
						return raw, nil
					}
					if u.Fragment != "" {
						target += "#" + u.Fragment
					}
					return target, nil
				})
				if err != nil {
					return nil, fmt.Errorf("product %s: %w", ref, err)
				}
			}
			destination := skill + "/" + targets[ref]
			if _, exists := contents[destination]; exists {
				return nil, fmt.Errorf("product destination collision %s", destination)
			}
			contents[destination] = output
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("undeclared product references (add exact imports, not folders):\n%s", strings.Join(missing, "\n"))
	}
	return provenance, nil
}

func validateLinks(contents map[string][]byte) error {
	anchors := map[string]map[string]bool{}
	links := map[string][]markdownLink{}
	for name, data := range contents {
		if strings.HasSuffix(name, ".md") {
			parsed, ids, err := markdownLinks(data)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			links[name], anchors[name] = parsed, ids
		}
	}
	for name, references := range links {
		for _, link := range references {
			u, err := localReference(link.target)
			if err != nil {
				return err
			}
			if u == nil {
				continue
			}
			root := strings.SplitN(name, "/", 2)[0]
			target := name
			if u.Path != "" {
				target = path.Join(root, u.Path)
			}
			if !strings.HasPrefix(target, root+"/") {
				return fmt.Errorf("link escapes skill root: %s -> %s", name, link.target)
			}
			if _, ok := contents[target]; !ok {
				return fmt.Errorf("unresolved skill-root link: %s -> %s", name, link.target)
			}
			if u.Fragment != "" && !anchors[target][u.Fragment] {
				return fmt.Errorf("unresolved heading: %s -> %s", name, link.target)
			}
		}
	}
	return nil
}
