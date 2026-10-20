package engine

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/datly/runtime/registry"
	xshape "github.com/viant/x/shape"
)

// ValidateGroupedSeedPaths checks dispatcher-selected paths before filtering or
// cloning. Bindly independently checks the canonical plan and grouped writes.
// Seeded authentication is not verification and cannot satisfy group barriers.
func ValidateGroupedSeedPaths(contract *registry.RouteInputContract, paths []string) error {
	fields := contract.Fields()
	grouped := false
	for _, field := range fields {
		if group := field.Binding().ResolutionGroup; group != nil {
			grouped = true
			if group.Mode != bindly.ResolutionGroupDependencies {
				return fmt.Errorf("reader seeds require dependency-mode resolution groups")
			}
		}
	}
	if !grouped {
		return nil
	}
	type protectedField struct {
		path          string
		indexes       [][]int
		authorization bool
	}
	var protected []protectedField
	byPath := map[string]registry.InputField{}
	for _, field := range fields {
		byPath[field.Path()] = field
		binding := field.Binding()
		authorization := field.VerifiesJWT() || binding.ErrorCode == 401 || binding.ErrorCode == 403
		if binding.ResolutionGroup == nil && !authorization {
			continue
		}
		indexes, err := groupedSeedWrites(contract.Type(), field)
		if err != nil {
			return err
		}
		protected = append(protected, protectedField{field.Path(), indexes, authorization})
	}
	seen := map[string]bool{}
	for _, path := range paths {
		field, found := byPath[path]
		if !found || seen[path] {
			return fmt.Errorf("invalid or duplicate grouped reader seed path %q", path)
		}
		seen[path] = true
		writes, err := groupedSeedWrites(contract.Type(), field)
		if err != nil {
			return err
		}
		for _, target := range protected {
			for _, write := range writes {
				for _, index := range target.indexes {
					if groupedSeedOverlap(write, index) {
						if target.authorization {
							return fmt.Errorf("reader seed %s overlaps authorization binding %s; verification must run normally", path, target.path)
						}
						return fmt.Errorf("reader seed %s overlaps resolution group binding %s", path, target.path)
					}
				}
			}
		}
	}
	return nil
}

func groupedSeedWrites(typ reflect.Type, field registry.InputField) ([][]int, error) {
	member, err := xshape.Linked(typ).StructField(field.Path())
	if err != nil {
		return nil, err
	}
	result := [][]int{member.Index}
	marker := field.Binding().MarkerField
	if marker == "" {
		// Route metadata may predate Bindly's inferred marker paths. Match its
		// setMarker convention without deriving authority from marker values.
		leaf := field.Path()[strings.LastIndex(field.Path(), ".")+1:]
		for _, candidate := range reflect.VisibleFields(typ) {
			if candidate.Tag.Get("setMarker") != "true" {
				continue
			}
			path := candidate.Name + "." + leaf
			if f, e := xshape.Linked(typ).StructField(path); e == nil && f.Type.Kind() == reflect.Bool {
				marker = path
				break
			}
		}
	}
	if marker != "" {
		member, err := xshape.Linked(typ).StructField(marker)
		if err != nil {
			return nil, err
		}
		result = append(result, member.Index)
	}
	return result, nil
}

func groupedSeedOverlap(left, right []int) bool {
	for i := 0; i < len(left) && i < len(right); i++ {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
