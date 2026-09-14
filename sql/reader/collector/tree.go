package collector

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	sqlxio "github.com/viant/sqlx/io"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xunsafe"
)

// TreePlan is immutable registration-compiled state for one self-referencing
// view. Declarative Holder/Child/Parent metadata remains on the canonical spec snapshot.
type TreePlan struct {
	slice       *xunsafe.Slice
	idField     *xunsafe.Field
	parentField *xunsafe.Field
	holderField *xunsafe.Field
	holderSlice *xunsafe.Slice
	holder      *xshape.Accessor
}

func NewTreePlan(view *View) (*TreePlan, error) {
	if view == nil || view.Spec.SelfReference == nil {
		return nil, nil
	}
	rType := view.Schema.RowType()
	if rType == nil || rType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("view %s self-reference requires a struct row type", view.Spec.Name)
	}
	reference := view.Spec.SelfReference
	idField, err := treeField(view, rType, reference.Child)
	if err != nil {
		return nil, fmt.Errorf("view %s self-reference child: %w", view.Spec.Name, err)
	}
	parentField, err := treeField(view, rType, reference.Parent)
	if err != nil {
		return nil, fmt.Errorf("view %s self-reference parent: %w", view.Spec.Name, err)
	}
	holderField := xunsafe.FieldByName(rType, strings.TrimSpace(reference.Holder))
	if holderField == nil {
		return nil, fmt.Errorf("view %s self-reference holder %q was not found on %s", view.Spec.Name, reference.Holder, rType)
	}
	if holderField.Type.Kind() != reflect.Slice {
		return nil, fmt.Errorf("view %s self-reference holder %s must be a slice, got %s", view.Spec.Name, reference.Holder, holderField.Type)
	}
	if !treeHolderAccepts(holderField.Type.Elem(), rType) {
		return nil, fmt.Errorf("view %s self-reference holder %s element %s cannot hold %s rows", view.Spec.Name, reference.Holder, holderField.Type.Elem(), rType)
	}
	holder, err := xshape.Linked(rType).Accessor(reference.Holder)
	if err != nil {
		return nil, err
	}
	return &TreePlan{
		slice:       view.Schema.Slice(),
		idField:     idField,
		parentField: parentField,
		holderField: holderField,
		holderSlice: xunsafe.NewSlice(holderField.Type),
		holder:      holder,
	}, nil
}

func treeField(view *View, rType reflect.Type, name string) (*xunsafe.Field, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("field is required")
	}
	fieldName := name
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		if strings.EqualFold(name, column.Name) || strings.EqualFold(name, column.Column) {
			fieldName = column.Name
			break
		}
	}
	field := xunsafe.FieldByName(rType, fieldName)
	if field == nil {
		return nil, fmt.Errorf("field or column %q was not found on %s", name, rType)
	}
	return field, nil
}

func treeHolderAccepts(holderElem, rowType reflect.Type) bool {
	if holderElem.Kind() == reflect.Interface {
		return true
	}
	if holderElem.Kind() == reflect.Ptr {
		holderElem = holderElem.Elem()
	}
	return holderElem == rowType
}

// Build connects direct children to parent rows and returns the roots in input
// order. Missing-parent rows are roots, matching the original reader behavior.
func (p *TreePlan) Build(nodes interface{}) interface{} {
	result, _ := p.build(nodes, nil)
	return result
}

func (p *TreePlan) build(nodes interface{}, evidence []*rowEvidence) (interface{}, []*rowEvidence) {
	nodesPtr := xunsafe.AsPointer(nodes)
	if p == nil || nodesPtr == nil {
		return nodes, evidence
	}
	count := p.slice.Len(nodesPtr)
	index := make(map[interface{}]int, count)
	for i := 0; i < count; i++ {
		p.clearHolder(nodesPtr, i)
		if i < len(evidence) && evidence[i] != nil {
			evidence[i].relations[p.holderField.Name] = nil
		}
		key := p.keyAt(nodesPtr, i, p.idField)
		if key != nil {
			index[key] = i
		}
	}

	childrenByParent := make(map[int][]int)
	seen := make(map[[2]int]struct{}, count)
	roots := make([]int, 0, count)
	for i := 0; i < count; i++ {
		parentKey := p.keyAt(nodesPtr, i, p.parentField)
		parentIndex, found := index[parentKey]
		if parentKey == nil || !found {
			roots = append(roots, i)
			continue
		}
		childKey := p.keyAt(nodesPtr, i, p.idField)
		if childKey == nil {
			roots = append(roots, i)
			continue
		}
		childIndex := index[childKey]
		edge := [2]int{parentIndex, childIndex}
		if _, ok := seen[edge]; ok {
			continue
		}
		seen[edge] = struct{}{}
		childrenByParent[parentIndex] = append(childrenByParent[parentIndex], childIndex)
	}

	state := make([]uint8, count)
	var hydrate func(int)
	hydrate = func(position int) {
		if state[position] == 2 {
			return
		}
		state[position] = 1
		parent := p.slice.ValuePointerAt(nodesPtr, position)
		for _, childPosition := range childrenByParent[position] {
			if state[childPosition] == 1 {
				continue
			}
			hydrate(childPosition)
			p.appendChild(parent, p.slice.ValuePointerAt(nodesPtr, childPosition))
			if position < len(evidence) && childPosition < len(evidence) && evidence[position] != nil {
				evidence[position].relations[p.holderField.Name] = append(evidence[position].relations[p.holderField.Name], evidence[childPosition])
			}
		}
		state[position] = 2
	}
	for i := 0; i < count; i++ {
		hydrate(i)
	}

	result := reflect.New(p.slice.Type)
	resultAppender := p.slice.Appender(unsafe.Pointer(result.Pointer()))
	var rootEvidence []*rowEvidence
	for _, position := range roots {
		resultAppender.Append(p.slice.ValuePointerAt(nodesPtr, position))
		if evidence != nil {
			if position < len(evidence) {
				rootEvidence = append(rootEvidence, evidence[position])
			} else {
				rootEvidence = append(rootEvidence, nil)
			}
		}
	}
	return result.Interface(), rootEvidence
}

func (p *TreePlan) clearHolder(nodesPtr unsafe.Pointer, index int) {
	row := p.slice.ValuePointerAt(nodesPtr, index)
	p.holderField.SetValue(xunsafe.AsPointer(row), reflect.Zero(p.holderField.Type).Interface())
}

func (p *TreePlan) appendChild(parent, child interface{}) {
	parentPtr := xunsafe.AsPointer(parent)
	if values, ok := p.holderField.Value(parentPtr).([]interface{}); ok {
		p.holderField.SetValue(parentPtr, append(values, child))
		return
	}
	p.holderSlice.Appender(p.holderField.ValuePointer(parentPtr)).Append(child)
}

func (p *TreePlan) keyAt(nodesPtr unsafe.Pointer, index int, field *xunsafe.Field) interface{} {
	row := p.slice.ValuePointerAt(nodesPtr, index)
	return sqlxio.NormalizeKey(field.Value(xunsafe.AsPointer(row)))
}
