package bootstrap

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

func mergeDescriptorImports(component *spec.Component, descriptor *x.Type) error {
	if component == nil || descriptor == nil || descriptor.SynteticType == nil {
		return nil
	}
	if component.TypeContext == nil {
		component.TypeContext = &spec.TypeContext{}
	}
	seen := map[string]bool{}
	aliasesByPackage := map[string]string{}
	for _, item := range component.TypeContext.Imports {
		alias := strings.TrimSpace(item.Alias)
		packagePath := strings.TrimSpace(item.Package)
		seen[alias+"\x00"+packagePath] = true
		if alias != "" {
			aliasesByPackage[alias] = packagePath
		}
	}
	aliases := make([]string, 0, len(descriptor.SynteticType.Imports))
	for alias := range descriptor.SynteticType.Imports {
		aliases = append(aliases, alias)
	}
	sort.Strings(aliases)
	for _, alias := range aliases {
		item := descriptor.SynteticType.Imports[alias]
		if item == nil || strings.TrimSpace(item.Path) == "" {
			continue
		}
		resolvedAlias := strings.TrimSpace(item.Alias)
		if resolvedAlias == "" {
			resolvedAlias = strings.TrimSpace(alias)
		}
		if resolvedAlias == "" {
			resolvedAlias = path.Base(strings.TrimSpace(item.Path))
		}
		packagePath := strings.TrimSpace(item.Path)
		if existing := aliasesByPackage[resolvedAlias]; existing != "" && existing != packagePath {
			return fmt.Errorf("import alias %q refers to both %q and %q", resolvedAlias, existing, packagePath)
		}
		aliasesByPackage[resolvedAlias] = packagePath
		key := resolvedAlias + "\x00" + packagePath
		if seen[key] {
			continue
		}
		seen[key] = true
		component.TypeContext.Imports = append(component.TypeContext.Imports, spec.ImportSpec{Alias: resolvedAlias, Package: packagePath})
	}
	return nil
}
