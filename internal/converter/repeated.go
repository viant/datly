package converter

import (
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

type Repeated []string

func (r Repeated) AsInts() ([]int, error) {
	var result = make([]int, 0, len(r))
	for _, item := range r {
		// Try to parse as float first to handle scientific notation
		if f, err := strconv.ParseFloat(item, 64); err == nil {
			result = append(result, int(f))
		} else {
			// Fall back to Atoi for regular integers
			v, err := strconv.Atoi(item)
			if err != nil {
				return nil, fmt.Errorf("failed to convert %v into %T, %w", r, result, err)
			}
			result = append(result, v)
		}
	}
	return result, nil
}

func (r Repeated) AsUInts() ([]uint, error) {
	v, err := r.AsInts()
	if err != nil {
		return nil, err
	}
	return *(*[]uint)(unsafe.Pointer(&v)), nil
}

func (r Repeated) AsInt64s() ([]int64, error) {
	v, err := r.AsInts()
	if err != nil {
		return nil, err
	}
	return *(*[]int64)(unsafe.Pointer(&v)), nil
}

func (r Repeated) AsUInt64s() ([]uint64, error) {
	v, err := r.AsInts()
	if err != nil {
		return nil, err
	}
	return *(*[]uint64)(unsafe.Pointer(&v)), nil
}

func (r Repeated) AsFloats64() ([]float64, error) {
	var result = make([]float64, 0, len(r))
	for _, item := range r {
		v, err := strconv.ParseFloat(item, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v into %T, %w", r, result, err)
		}
		result = append(result, v)
	}
	return result, nil
}

func (r Repeated) AsBools() ([]bool, error) {
	var result = make([]bool, 0, len(r))
	for _, item := range r {
		v, err := strconv.ParseBool(item)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v into %T, %w", r, result, err)
		}
		result = append(result, v)
	}
	return result, nil
}

func (r Repeated) AsFloats32() ([]float32, error) {
	var result = make([]float32, 0, len(r))
	for _, item := range r {
		v, err := strconv.ParseFloat(item, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v into %T, %w", r, result, err)
		}
		result = append(result, float32(v))
	}
	return result, nil
}

func NewRepeated(text string, trimSpace bool) Repeated {
	if text == "" {
		return Repeated{}
	}
	if text[0] == '[' && text[len(text)-1] == ']' { //remove enclosure if needed
		text = text[1 : len(text)-1]
	}
	elements := strings.Split(text, ",")
	if !trimSpace {
		return elements
	}
	var result = make(Repeated, 0, len(elements))
	for _, elem := range elements {
		if trimSpace {
			if elem = strings.TrimSpace(elem); elem == "" {
				continue
			}
		}
		result = append(result, elem)
	}
	return result
}
