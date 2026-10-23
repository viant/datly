package transcribe

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/transcribe/internal/discoverytype"
	"github.com/viant/datly/typecatalog"
)

// RuntimeArtifactType describes one exact runtime type. Named Go types are
// references to compiled host identities; they are never replaced by structs.
type RuntimeArtifactType struct {
	ID          string                 `json:"id"`
	Kind        string                 `json:"kind"`
	Package     string                 `json:"package,omitempty"`
	Name        string                 `json:"name,omitempty"`
	Fingerprint string                 `json:"fingerprint,omitempty"`
	Element     string                 `json:"element,omitempty"`
	Key         string                 `json:"key,omitempty"`
	Length      int                    `json:"length,omitempty"`
	Fields      []RuntimeArtifactField `json:"fields,omitempty"`
	Source      string                 `json:"source,omitempty"`
}

type RuntimeArtifactField struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Tag       string `json:"tag,omitempty"`
	Anonymous bool   `json:"anonymous,omitempty"`
}

type artifactTypeEncoder struct {
	ids      map[reflect.Type]string
	artifact *RuntimeContractArtifact
}

func (e *artifactTypeEncoder) add(t reflect.Type) (string, error) {
	if t == nil {
		return "", fmt.Errorf("nil runtime type")
	}
	if id := e.ids[t]; id != "" {
		return id, nil
	}
	id := "t" + strconv.Itoa(len(e.artifact.Types))
	e.ids[t] = id
	index := len(e.artifact.Types)
	e.artifact.Types = append(e.artifact.Types, RuntimeArtifactType{})
	n := RuntimeArtifactType{ID: id, Kind: t.Kind().String()}
	var err error
	if t.Name() != "" {
		n.Name, n.Package = t.Name(), t.PkgPath()
		if n.Package != "" {
			n.Kind = "named"
			n.Fingerprint = RuntimeTypeFingerprint(t)
		} else if artifactBuiltin(n.Name) != t {
			return "", fmt.Errorf("unsupported builtin type %v", t)
		} else {
			n.Kind = "builtin"
		}
	} else {
		switch t.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Array:
			n.Element, err = e.add(t.Elem())
			if t.Kind() == reflect.Array {
				n.Length = t.Len()
			}
		case reflect.Map:
			n.Key, err = e.add(t.Key())
			if err == nil {
				n.Element, err = e.add(t.Elem())
			}
		case reflect.Struct:
			n.Kind = "x-struct"
			n.Source, err = renderArtifactXType(t)
		case reflect.Interface:
			if t.NumMethod() != 0 {
				return "", fmt.Errorf("unnamed interface with methods cannot be persisted")
			}
		default:
			return "", fmt.Errorf("unsupported structural runtime type %v", t)
		}
	}
	if err != nil {
		return "", err
	}
	e.artifact.Types[index] = n
	return id, nil
}

type artifactTypeDecoder struct {
	catalog  *typecatalog.Catalog
	nodes    map[string]RuntimeArtifactType
	types    map[string]reflect.Type
	building map[string]bool
}

func (d *artifactTypeDecoder) materialize(id string) (result reflect.Type, err error) {
	if t := d.types[id]; t != nil {
		return t, nil
	}
	n, found := d.nodes[id]
	if !found {
		return nil, fmt.Errorf("artifact type %q is missing", id)
	}
	if d.building[id] {
		return nil, fmt.Errorf("recursive structural artifact type %q is unsupported", id)
	}
	d.building[id] = true
	defer func() {
		delete(d.building, id)
		if p := recover(); p != nil {
			result = nil
			err = fmt.Errorf("invalid artifact type %q: %v", id, p)
		}
	}()
	switch n.Kind {
	case "named":
		if n.Package == "" || n.Name == "" || n.Fingerprint == "" {
			return nil, fmt.Errorf("artifact named type %q lacks identity", id)
		}
		key := n.Package + "." + n.Name
		candidate := discoverytype.StandardType(key)
		if candidate != nil && RuntimeTypeFingerprint(candidate) == n.Fingerprint {
			result = candidate
			break
		}
		for _, authority := range []typecatalog.Authority{typecatalog.PackageAuthority, typecatalog.TranscribeAuthority} {
			candidate, _, err = d.catalog.ResolveRuntimeType(authority, key)
			if err != nil {
				return nil, err
			}
			if candidate != nil && candidate.PkgPath() == n.Package && candidate.Name() == n.Name && RuntimeTypeFingerprint(candidate) == n.Fingerprint {
				result = candidate
				break
			}
		}
		// Catalog aliases may retain a different logical package/name while
		// linking this exact compiled Go identity.
		if result == nil {
			for _, authority := range []typecatalog.Authority{typecatalog.PackageAuthority, typecatalog.TranscribeAuthority} {
				registry, e := d.catalog.Registry(authority)
				if e != nil {
					return nil, e
				}
				for _, alias := range registry.Keys() {
					candidate = registry.Lookup(alias).Type
					if candidate != nil && candidate.PkgPath() == n.Package && candidate.Name() == n.Name && RuntimeTypeFingerprint(candidate) == n.Fingerprint {
						result = candidate
						break
					}
				}
				if result != nil {
					break
				}
			}
		}
		if result == nil {
			return nil, fmt.Errorf("artifact named type %q requires a compatible compiled host identity", key)
		}
	case "builtin":
		result = artifactBuiltin(n.Name)
		if result == nil {
			return nil, fmt.Errorf("unsupported artifact builtin %q", n.Name)
		}
	case "ptr", "slice", "array":
		element, e := d.materialize(n.Element)
		if e != nil {
			return nil, e
		}
		switch n.Kind {
		case "ptr":
			result = reflect.PointerTo(element)
		case "slice":
			result = reflect.SliceOf(element)
		case "array":
			result = reflect.ArrayOf(n.Length, element)
		}
	case "map":
		key, e := d.materialize(n.Key)
		if e != nil {
			return nil, e
		}
		element, e := d.materialize(n.Element)
		if e != nil {
			return nil, e
		}
		result = reflect.MapOf(key, element)
	case "x-struct":
		result, err = materializeArtifactXType(n.Source, d.catalog)
		if err != nil {
			return nil, err
		}
	case "interface":
		result = reflect.TypeFor[any]()
	default:
		return nil, fmt.Errorf("unsupported artifact type kind %q", n.Kind)
	}
	d.types[id] = result
	return result, nil
}

func artifactBuiltin(name string) reflect.Type {
	switch name {
	case "error":
		return reflect.TypeFor[error]()
	case "bool":
		return reflect.TypeFor[bool]()
	case "string":
		return reflect.TypeFor[string]()
	case "int":
		return reflect.TypeFor[int]()
	case "int8":
		return reflect.TypeFor[int8]()
	case "int16":
		return reflect.TypeFor[int16]()
	case "int32":
		return reflect.TypeFor[int32]()
	case "int64":
		return reflect.TypeFor[int64]()
	case "uint":
		return reflect.TypeFor[uint]()
	case "uint8":
		return reflect.TypeFor[uint8]()
	case "uint16":
		return reflect.TypeFor[uint16]()
	case "uint32":
		return reflect.TypeFor[uint32]()
	case "uint64":
		return reflect.TypeFor[uint64]()
	case "uintptr":
		return reflect.TypeFor[uintptr]()
	case "float32":
		return reflect.TypeFor[float32]()
	case "float64":
		return reflect.TypeFor[float64]()
	case "complex64":
		return reflect.TypeFor[complex64]()
	case "complex128":
		return reflect.TypeFor[complex128]()
	}
	return nil
}

// RuntimeTypeFingerprint returns the same type identity used by portable
// reader artifacts. It includes reachable layouts and method signatures, so a
// host can bind its linked seed catalog without duplicating this traversal.
// A nil type has no fingerprint and must not be used for artifact admission.
func RuntimeTypeFingerprint(t reflect.Type) string {
	if t == nil {
		return ""
	}
	return artifactNamedFingerprint(t)
}

// Include reachable layouts and method signatures, with cycle references, so a
// host binary cannot silently substitute a different build of a named type.
func artifactNamedFingerprint(t reflect.Type) string {
	var b strings.Builder
	visited := map[reflect.Type]int{}
	var visit func(reflect.Type)
	visit = func(t reflect.Type) {
		if id, ok := visited[t]; ok {
			fmt.Fprintf(&b, "ref:%d;", id)
			return
		}
		visited[t] = len(visited)
		fmt.Fprintf(&b, "%s:%q:%q;", t.Kind(), t.PkgPath(), t.Name())
		switch t.Kind() {
		case reflect.Struct:
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				fmt.Fprintf(&b, "field:%q:%q:%q:%t;", f.Name, f.PkgPath, string(f.Tag), f.Anonymous)
				visit(f.Type)
			}
		case reflect.Pointer, reflect.Slice, reflect.Chan:
			if t.Kind() == reflect.Chan {
				fmt.Fprintf(&b, "direction:%d;", t.ChanDir())
			}
			visit(t.Elem())
		case reflect.Array:
			fmt.Fprintf(&b, "length:%d;", t.Len())
			visit(t.Elem())
		case reflect.Map:
			visit(t.Key())
			visit(t.Elem())
		case reflect.Func:
			fmt.Fprintf(&b, "func:%d:%d:%t;", t.NumIn(), t.NumOut(), t.IsVariadic())
			for i := 0; i < t.NumIn(); i++ {
				visit(t.In(i))
			}
			for i := 0; i < t.NumOut(); i++ {
				visit(t.Out(i))
			}
		}
		for i := 0; i < t.NumMethod(); i++ {
			m := t.Method(i)
			fmt.Fprintf(&b, "method:%q:%q;", m.PkgPath, m.Name)
			visit(m.Type)
		}
	}
	visit(t)
	if t.Kind() != reflect.Interface {
		visit(reflect.PointerTo(t))
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
