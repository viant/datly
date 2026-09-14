package velty

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/viant/xunsafe"
)

type indexNode struct {
	children map[any]*indexNode
	terminal bool
}

type indexEntry struct {
	root   *indexNode
	record indexRecord
}

type indexRecord struct {
	name         string
	valueToken   unsafe.Pointer
	pointerToken unsafe.Pointer
	fields       []indexField
}

type indexField struct {
	field        *xunsafe.Field
	pointerDepth int
	valueType    *xunsafe.Type
	normalize    func(unsafe.Pointer) any
}

type interfaceHeader struct {
	typeToken unsafe.Pointer
	data      unsafe.Pointer
}

// Index owns named presence indexes for one Velty handler invocation.
type Index struct {
	entries map[string]indexEntry
}

func newIndex() *Index {
	return &Index{entries: map[string]indexEntry{}}
}

// Build compiles ordered current/record field pairs and indexes the current
// typed slice. Reflection is confined to this one setup call; Has uses the
// compiled record accessor for every traversal lookup.
func (i *Index) Build(name string, current any, input any, recordPath string, fieldPairs ...string) (string, error) {
	if i == nil || i.entries == nil {
		return "", fmt.Errorf("velty index capability is not configured")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("velty index name is required")
	}
	if len(fieldPairs) == 0 || len(fieldPairs)%2 != 0 {
		return "", fmt.Errorf("velty index %q requires ordered current/record field pairs", name)
	}
	if _, ok := i.entries[name]; ok {
		return "", fmt.Errorf("velty index %q is already built", name)
	}

	currentValue := reflect.ValueOf(current)
	if !currentValue.IsValid() || currentValue.Kind() != reflect.Slice {
		return "", fmt.Errorf("velty index %q requires a current slice, got %T", name, current)
	}
	currentType, currentPointerDepth, err := indexStructType(currentValue.Type().Elem())
	if err != nil {
		return "", fmt.Errorf("velty index %q current rows: %w", name, err)
	}
	recordType, err := indexRecordType(input, recordPath)
	if err != nil {
		return "", fmt.Errorf("velty index %q record path: %w", name, err)
	}

	partCount := len(fieldPairs) / 2
	currentFields := make([]indexField, partCount)
	recordFields := make([]indexField, partCount)
	seenCurrent := make(map[string]struct{}, partCount)
	seenRecord := make(map[string]struct{}, partCount)
	for position := 0; position < partCount; position++ {
		currentName := strings.TrimSpace(fieldPairs[position*2])
		recordName := strings.TrimSpace(fieldPairs[position*2+1])
		if err = uniqueIndexField(name, "current", currentName, seenCurrent); err != nil {
			return "", err
		}
		if err = uniqueIndexField(name, "record", recordName, seenRecord); err != nil {
			return "", err
		}
		currentFields[position], err = compileIndexField(currentType, currentName)
		if err != nil {
			return "", fmt.Errorf("velty index %q current field %q: %w", name, currentName, err)
		}
		recordFields[position], err = compileIndexField(recordType, recordName)
		if err != nil {
			return "", fmt.Errorf("velty index %q record field %q: %w", name, recordName, err)
		}
		if !sameIndexValueType(currentFields[position], recordFields[position]) {
			return "", fmt.Errorf("velty index %q fields %q and %q have incompatible types %s and %s",
				name, currentName, recordName, currentFields[position].valueType.Type(), recordFields[position].valueType.Type())
		}
	}

	entry := indexEntry{
		root: &indexNode{},
		record: indexRecord{
			name:         recordType.String(),
			valueToken:   interfaceTypeToken(reflect.New(recordType).Elem().Interface()),
			pointerToken: interfaceTypeToken(reflect.New(recordType).Interface()),
			fields:       recordFields,
		},
	}
	rows := xunsafe.NewSlice(currentValue.Type())
	slicePointer := xunsafe.AsPointer(current)
	for position := 0; position < rows.Len(slicePointer); position++ {
		owner, ok := indexRowOwner(rows.PointerAt(slicePointer, uintptr(position)), currentPointerDepth)
		if !ok {
			continue
		}
		parts, complete := readIndexParts(owner, currentFields)
		if !complete {
			continue
		}
		if err = entry.add(parts); err != nil {
			return "", fmt.Errorf("velty index %q item %d: %w", name, position, err)
		}
	}
	i.entries[name] = entry
	return "", nil
}

// Has reports whether the ordered key fields of record exist in the named
// current index. Its lookup path contains no reflection or field discovery.
func (i *Index) Has(name string, record any) (bool, error) {
	if i == nil || i.entries == nil {
		return false, fmt.Errorf("velty index capability is not configured")
	}
	entry, ok := i.entries[strings.TrimSpace(name)]
	if !ok {
		return false, fmt.Errorf("velty index %q was not built", name)
	}
	owner, available, err := entry.record.owner(record)
	if err != nil || !available {
		return false, err
	}
	node := entry.root
	for _, field := range entry.record.fields {
		part, available := field.value(owner)
		if !available {
			return false, nil
		}
		if node.children == nil {
			return false, nil
		}
		node = node.children[part]
		if node == nil {
			return false, nil
		}
	}
	return node.terminal, nil
}

func (e indexEntry) add(parts []any) error {
	node := e.root
	for _, part := range parts {
		if node.children == nil {
			node.children = map[any]*indexNode{}
		}
		next := node.children[part]
		if next == nil {
			next = &indexNode{}
			node.children[part] = next
		}
		node = next
	}
	if node.terminal {
		return fmt.Errorf("duplicate compound key")
	}
	node.terminal = true
	return nil
}

func (r indexRecord) owner(value any) (unsafe.Pointer, bool, error) {
	if value == nil {
		return nil, false, nil
	}
	header := (*interfaceHeader)(unsafe.Pointer(&value))
	switch header.typeToken {
	case r.pointerToken:
		if header.data == nil {
			return nil, false, nil
		}
		return header.data, true, nil
	case r.valueToken:
		return xunsafe.AsPointer(value), true, nil
	default:
		return nil, false, fmt.Errorf("velty index record requires %s or *%s, got %T", r.name, r.name, value)
	}
}

func indexRecordType(input any, path string) (reflect.Type, error) {
	if input == nil {
		return nil, fmt.Errorf("input is required")
	}
	current, _, err := indexStructType(reflect.TypeOf(input))
	if err != nil {
		return nil, err
	}
	parts := strings.Split(strings.TrimSpace(path), "/")
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("path is required")
	}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("path %q contains an empty field", path)
		}
		field, ok := current.FieldByName(part)
		if !ok {
			return nil, fmt.Errorf("field %q was not found in %s", part, current)
		}
		current, _, err = indexStructType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", part, err)
		}
	}
	return current, nil
}

func indexStructType(value reflect.Type) (reflect.Type, int, error) {
	pointerDepth := 0
	for value.Kind() == reflect.Ptr || value.Kind() == reflect.Slice {
		if value.Kind() == reflect.Ptr {
			pointerDepth++
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, 0, fmt.Errorf("requires a struct shape, got %s", value)
	}
	return value, pointerDepth, nil
}

func compileIndexField(owner reflect.Type, name string) (indexField, error) {
	field := xunsafe.FieldByName(owner, name)
	if field == nil {
		return indexField{}, fmt.Errorf("field was not found in %s", owner)
	}
	valueType := field.Type
	pointerDepth := 0
	for valueType.Kind() == reflect.Ptr {
		pointerDepth++
		valueType = valueType.Elem()
	}
	if valueType.Kind() == reflect.Interface {
		return indexField{}, fmt.Errorf("interface key type %s is not supported", valueType)
	}
	normalize := indexNormalizer(valueType)
	if normalize == nil && !valueType.Comparable() {
		return indexField{}, fmt.Errorf("key type %s is not comparable", valueType)
	}
	return indexField{
		field: field, pointerDepth: pointerDepth,
		valueType: xunsafe.NewType(valueType), normalize: normalize,
	}, nil
}

func (f indexField) value(owner unsafe.Pointer) (any, bool) {
	pointer := f.field.Pointer(owner)
	for depth := 0; depth < f.pointerDepth; depth++ {
		pointer = *(*unsafe.Pointer)(pointer)
		if pointer == nil {
			return nil, false
		}
	}
	if f.normalize != nil {
		return f.normalize(pointer), true
	}
	return f.valueType.Interface(pointer), true
}

func indexNormalizer(valueType reflect.Type) func(unsafe.Pointer) any {
	if valueType.PkgPath() != "" {
		return nil
	}
	switch valueType.Kind() {
	case reflect.Int16:
		return func(pointer unsafe.Pointer) any { return int(*(*int16)(pointer)) }
	case reflect.Int32:
		return func(pointer unsafe.Pointer) any { return int(*(*int32)(pointer)) }
	case reflect.Int64:
		return func(pointer unsafe.Pointer) any { return int(*(*int64)(pointer)) }
	case reflect.Slice:
		if valueType == reflect.TypeFor[[]byte]() {
			return func(pointer unsafe.Pointer) any { return string(*(*[]byte)(pointer)) }
		}
	}
	return nil
}

func readIndexParts(owner unsafe.Pointer, fields []indexField) ([]any, bool) {
	parts := make([]any, len(fields))
	for position, field := range fields {
		value, available := field.value(owner)
		if !available {
			return nil, false
		}
		parts[position] = value
	}
	return parts, true
}

func indexRowOwner(pointer unsafe.Pointer, pointerDepth int) (unsafe.Pointer, bool) {
	for depth := 0; depth < pointerDepth; depth++ {
		pointer = *(*unsafe.Pointer)(pointer)
		if pointer == nil {
			return nil, false
		}
	}
	return pointer, true
}

func uniqueIndexField(indexName, side, fieldName string, seen map[string]struct{}) error {
	if fieldName == "" {
		return fmt.Errorf("velty index %q has an empty %s field", indexName, side)
	}
	if _, ok := seen[fieldName]; ok {
		return fmt.Errorf("velty index %q repeats %s field %q", indexName, side, fieldName)
	}
	seen[fieldName] = struct{}{}
	return nil
}

func sameIndexValueType(left, right indexField) bool {
	return left.valueType.Type() == right.valueType.Type()
}

func interfaceTypeToken(value any) unsafe.Pointer {
	return (*interfaceHeader)(unsafe.Pointer(&value)).typeToken
}
