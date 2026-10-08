package dml

import (
	"fmt"
	"github.com/viant/datly/internal/drainowner"
	"reflect"

	rhandler "github.com/viant/datly/runtime/handler"
	sqlio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/insert"
	"github.com/viant/sqlx/option"
	"github.com/viant/structology"
	"github.com/viant/xunsafe"
)

func (d *Data) InsertWithQueueContract(table string, data any, contract rhandler.QueueContract) error {
	return d.appendWithQueueContract(table, data, dataOpInsert, contract)
}

func (d *Data) DeleteWithQueueContract(table string, data any, contract rhandler.QueueContract) error {
	if contract != rhandler.SourceRow {
		return fmt.Errorf("DELETE queue contract requires source-row")
	}
	return d.appendWithQueueContract(table, data, dataOpDelete, contract)
}

func (d *Data) appendWithQueueContract(table string, data any, kind dataOperationKind, contract rhandler.QueueContract) error {
	v := reflect.ValueOf(data)
	var payload reflect.Value
	switch contract {
	case rhandler.SourceRow:
		if v.IsValid() && v.Kind() == reflect.Pointer && !v.IsNil() {
			v = v.Elem()
		}
		if !v.IsValid() || v.Kind() != reflect.Struct {
			return fmt.Errorf("source-row requires a nonnil struct row")
		}
		payload = reflect.New(v.Type())
		payload.Elem().Set(v) // Exactly one shallow struct assignment.
	case rhandler.SourceSlice:
		if kind != dataOpInsert || !v.IsValid() || v.Kind() != reflect.Slice || v.Type().Elem().Kind() != reflect.Pointer || v.Type().Elem().Elem().Kind() != reflect.Struct {
			return fmt.Errorf("source-slice requires a typed slice of struct pointers")
		}
		for i := 0; i < v.Len(); i++ {
			if v.Index(i).IsNil() {
				return fmt.Errorf("source-slice row %d is nil", i)
			}
		}
		payload = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		reflect.Copy(payload, v) // Copy backing, retain all element identities.
	default:
		return fmt.Errorf("unknown queue contract %d", contract)
	}
	if kind == dataOpInsert {
		if payload.Kind() == reflect.Slice {
			for i := 0; i < payload.Len(); i++ {
				if payload.Index(i).Type().Implements(reflect.TypeFor[insert.Insertable]()) {
					return fmt.Errorf("queue contract does not support SQLX OnInsert callbacks")
				}
			}
		} else if payload.Type().Implements(reflect.TypeFor[insert.Insertable]()) {
			return fmt.Errorf("queue contract does not support SQLX OnInsert callbacks")
		}
		if contract == rhandler.SourceRow && queueNeedsGeneratedIDBackfill(payload.Interface()) {
			return fmt.Errorf("source-row INSERT requires native allocated identity before queue admission")
		}
		if contract == rhandler.SourceSlice {
			for i := 0; i < payload.Len(); i++ {
				if queueNeedsGeneratedIDBackfill(payload.Index(i).Interface()) {
					return fmt.Errorf("source-slice INSERT row %d requires native allocated identity before queue admission", i)
				}
			}
		}
	}
	evidence, err := captureQueuePayload(payload, kind)
	if err != nil {
		return err
	}
	operation := dataOperation{kind: kind, table: table, data: payload.Interface(), queueContract: contract, appendBarrier: true, payloadEvidence: evidence}
	owner := d.owner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if err = owner.appendableLocked(); err != nil {
		return err
	}
	if !d.open {
		return ErrComponentSealed
	}
	if err = operation.validatePayload(); err != nil {
		return owner.failProtectedMutationLocked(err)
	}
	if contract == rhandler.SourceSlice && payload.Len() == 0 {
		return nil
	}
	owner.nextOp++
	operation.id, operation.frame = owner.nextOp, d
	operation.journalFrame = d.journalFrame
	if err := drainowner.AppendJournal(owner, d.journalFrame, &operation); err != nil {
		owner.nextOp--
		return owner.failProtectedMutationLocked(err)
	}
	d.queue = append(d.queue, &operation)
	return nil
}

type mappedQueueHolder struct {
	paths   [][]int
	typ     reflect.Type
	pointer uintptr
}
type payloadFieldEvidence struct {
	paths   [][]int
	holders []mappedQueueHolder
	image   *queueValueImage
}
type payloadRowEvidence struct {
	row    reflect.Value
	fields []payloadFieldEvidence
}
type queuePayloadEvidence struct {
	rows     []payloadRowEvidence
	slice    reflect.Value
	elements []uintptr
}

func captureQueuePayload(payload reflect.Value, kind dataOperationKind) (*queuePayloadEvidence, error) {
	result := &queuePayloadEvidence{}
	rows := []reflect.Value{payload}
	if payload.Kind() == reflect.Slice {
		result.slice = payload
		rows = nil
		for i := 0; i < payload.Len(); i++ {
			rows = append(rows, payload.Index(i))
			result.elements = append(result.elements, payload.Index(i).Pointer())
		}
	}
	for _, row := range rows {
		columns, err := sqlio.StructColumns(row.Type(), option.IdentityOnly(kind == dataOpDelete))
		if err != nil {
			return nil, fmt.Errorf("queue payload mapping: %w", err)
		}
		evidence := payloadRowEvidence{row: reflect.ValueOf(row.Interface())}
		for _, column := range columns {
			fields, ok := column.(sqlio.ColumnWithFields)
			if !ok {
				return nil, fmt.Errorf("queue mapped column %s has no native field path", column.Name())
			}
			paths, err := compiledQueuePaths(row.Type(), fields.Fields())
			if err != nil {
				return nil, err
			}
			value, err := mappedQueueValue(row, paths)
			if err != nil {
				return nil, err
			}
			image, err := captureQueueValueForEncoding(value, 0, column.Tag() != nil && column.Tag().Encoding == sqlio.EncodingCSV)
			if err != nil {
				return nil, fmt.Errorf("queue column %s: %w", column.Name(), err)
			}
			holders, err := captureMappedQueueHolders(row, paths)
			if err != nil {
				return nil, err
			}
			evidence.fields = append(evidence.fields, payloadFieldEvidence{paths: paths, holders: holders, image: image})
		}
		// SQLX selectors can read a setMarker holder even though it is not a column.
		if kind == dataOpInsert {
			typ := row.Type().Elem()
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				if structology.IsSetMarker(field.Tag) {
					image, err := captureQueueValue(row.Elem().Field(i), 0)
					if err != nil {
						return nil, err
					}
					evidence.fields = append(evidence.fields, payloadFieldEvidence{paths: [][]int{{i}}, image: image})
				}
			}
		}
		result.rows = append(result.rows, evidence)
	}
	return result, nil
}

// Resolve SQLX's compiled offsets/types into structural paths. Folded value
// embeddings can shadow names; FieldByName is never a mapping authority.
func compiledQueuePaths(rowType reflect.Type, fields []*xunsafe.Field) ([][]int, error) {
	var result [][]int
	t := rowType
	for _, field := range fields {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Struct {
			return nil, fmt.Errorf("unsupported queue mapped holder %v", t)
		}
		var matches [][]int
		var walk func(reflect.Type, uintptr, []int)
		walk = func(typ reflect.Type, offset uintptr, path []int) {
			for i := 0; i < typ.NumField(); i++ {
				f := typ.Field(i)
				p := append(append([]int(nil), path...), i)
				if f.Offset == offset && f.Type == field.Type && f.Name == field.Name {
					matches = append(matches, p)
				}
				if f.Type.Kind() == reflect.Struct && offset >= f.Offset {
					walk(f.Type, offset-f.Offset, p)
				}
			}
		}
		walk(t, field.Offset, nil)
		if len(matches) != 1 {
			return nil, fmt.Errorf("queue compiled field %s offset %d type %v has %d structural locations", field.Name, field.Offset, field.Type, len(matches))
		}
		result = append(result, matches[0])
		t = field.Type
	}
	return result, nil
}
func mappedQueueValue(row reflect.Value, paths [][]int) (reflect.Value, error) {
	v := row
	for _, path := range paths {
		for v.IsValid() && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return v, nil
			}
			v = v.Elem()
		}
		if !v.IsValid() || v.Kind() != reflect.Struct {
			return reflect.Value{}, fmt.Errorf("queue mapped holder is invalid")
		}
		v = v.FieldByIndex(path)
	}
	return v, nil
}

func (o *dataOperation) validatePayload() error {
	if o.payloadEvidence == nil {
		return nil
	}
	e := o.payloadEvidence
	if e.slice.IsValid() {
		v := reflect.ValueOf(o.data)
		if v.Type() != e.slice.Type() || v.Len() != len(e.elements) || v.Pointer() != e.slice.Pointer() {
			return fmt.Errorf("queued source-slice membership changed")
		}
		for i, pointer := range e.elements {
			if v.Index(i).IsNil() || v.Index(i).Pointer() != pointer {
				return fmt.Errorf("queued source-slice row %d changed", i)
			}
		}
	}
	if !e.slice.IsValid() {
		v := reflect.ValueOf(o.data)
		if v.Type() != e.rows[0].row.Type() || v.Pointer() != e.rows[0].row.Pointer() {
			return fmt.Errorf("queued source-row storage changed")
		}
	}
	for _, row := range e.rows {
		for _, field := range row.fields {
			for _, holder := range field.holders {
				v, err := mappedQueueValue(row.row, holder.paths)
				if err != nil {
					return err
				}
				if v.Type() != holder.typ || v.Pointer() != holder.pointer {
					return fmt.Errorf("queued SQL mapped holder changed")
				}
			}
			value, err := mappedQueueValue(row.row, field.paths)
			if err != nil {
				return err
			}
			if !field.image.equal(value, 0) {
				return fmt.Errorf("queued SQL payload field %v changed", field.paths)
			}
		}
	}
	return nil
}

func hasQueuePayloadEvidence(operations []*dataOperation) bool {
	for _, operation := range operations {
		if operation.payloadEvidence != nil {
			return true
		}
	}
	return false
}

// Match SQLX numeric-updater admission and its native presence map without
// invoking a binder (which can produce execution-time encoded values).
func queueNeedsGeneratedIDBackfill(data any) bool {
	row := reflect.ValueOf(data)
	marker := &option.SetMarker{}
	columns, _, err := sqlio.StructColumnMapper(row.Type(), marker)
	if err != nil {
		return true
	}
	for position, column := range columns {
		if !sqlio.IsIdentityColumn(column) {
			continue
		}
		typ := column.ScanType()
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() == reflect.String {
			continue
		}
		fields, ok := column.(sqlio.ColumnWithFields)
		if !ok {
			return true
		}
		paths, err := compiledQueuePaths(row.Type(), fields.Fields())
		if err != nil {
			return true
		}
		identity, err := mappedQueueValue(row, paths)
		if err != nil {
			return true
		}
		for identity.IsValid() && (identity.Kind() == reflect.Pointer || identity.Kind() == reflect.Interface) {
			if identity.IsNil() {
				return true
			}
			identity = identity.Elem()
		}
		if !identity.IsValid() {
			return true
		}
		// SQLX explicitly supplied zero remains an assignment; nil never does.
		if marker.Marker != nil && marker.IsSet(xunsafe.AsPointer(data), position) {
			continue
		}
		// SQLX also installs numeric updaters for unsigned and other
		// non-string identities. Do not reuse the ordinary batching
		// predicate, which only recognizes signed integers.
		if identity.IsZero() {
			return true
		}
	}
	return false
}
func captureMappedQueueHolders(row reflect.Value, paths [][]int) ([]mappedQueueHolder, error) {
	var result []mappedQueueHolder
	for i := 1; i < len(paths); i++ {
		v, err := mappedQueueValue(row, paths[:i])
		if err != nil {
			return nil, err
		}
		if v.Kind() == reflect.Pointer {
			copyPaths := make([][]int, i)
			for j := range copyPaths {
				copyPaths[j] = append([]int(nil), paths[j]...)
			}
			result = append(result, mappedQueueHolder{paths: copyPaths, typ: v.Type(), pointer: v.Pointer()})
		}
	}
	return result, nil
}
