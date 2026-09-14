package tag

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	tagly "github.com/viant/tagly/tags"
)

const PredicateName = "predicate"

// PredicateValue formats canonical predicate metadata using ParsePredicates'
// grammar. Argument order remains authored order.
func PredicateValue(predicate *spec.Predicate) (string, error) {
	if predicate == nil {
		return "", nil
	}
	name := strings.TrimSpace(predicate.Name)
	if name == "" {
		return "", fmt.Errorf("predicate name is required")
	}
	if predicate.Group < 0 {
		return "", fmt.Errorf("predicate group must be a non-negative integer, got %d", predicate.Group)
	}
	values := []string{encodeScalarValue(name)}
	if predicate.Group > 0 {
		values = append(values, "group="+strconv.Itoa(predicate.Group))
	}
	if predicate.ApplyWhenAbsent {
		values = append(values, "applyWhenAbsent=true")
	}
	for _, argument := range predicate.Args {
		values = append(values, encodeScalarValue(argument))
	}
	return strings.Join(values, ","), nil
}

// ParsePredicates parses every predicate tag attached to one Go field.
func ParsePredicates(structTag reflect.StructTag) ([]*spec.Predicate, error) {
	var result []*spec.Predicate
	for _, item := range tagly.NewTags(string(structTag)) {
		if item == nil || item.Name != PredicateName {
			continue
		}
		name, values := item.Values.Name()
		name, err := decodeScalarValue(strings.TrimSpace(name))
		if err != nil {
			return nil, err
		}
		predicate := &spec.Predicate{Name: name}
		err = values.MatchPairs(func(key, value string) error {
			rawKey := strings.TrimSpace(key)
			rawValue := strings.TrimSpace(value)
			decodedKey, keyErr := decodeScalarValue(rawKey)
			switch strings.ToLower(decodedKey) {
			case "ensure":
				return fmt.Errorf("unsupported predicate option %q; use applyWhenAbsent", decodedKey)
			case "name":
				if keyErr != nil {
					break
				}
				if predicate.Name == "" {
					predicate.Name, err = decodeScalarValue(rawValue)
					if err != nil {
						return err
					}
					return nil
				}
			case "group":
				if keyErr != nil {
					break
				}
				value, err := decodeScalarValue(rawValue)
				if err != nil {
					return err
				}
				group, err := strconv.Atoi(value)
				if err != nil || group < 0 {
					return fmt.Errorf("predicate group must be a non-negative integer, got %q", value)
				}
				predicate.Group = group
				return nil
			case "applywhenabsent":
				if keyErr != nil {
					break
				}
				value, err := decodeScalarValue(rawValue)
				if err != nil {
					return err
				}
				applyWhenAbsent, err := parseBool("predicate applyWhenAbsent", value)
				if err != nil {
					return err
				}
				predicate.ApplyWhenAbsent = applyWhenAbsent
				return nil
			}
			argument := rawKey
			if rawValue != "" {
				argument += "=" + rawValue
			}
			argument, err = decodeScalarValue(argument)
			if err != nil {
				return err
			}
			predicate.Args = append(predicate.Args, argument)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if predicate.Name == "" {
			return nil, fmt.Errorf("predicate name is required")
		}
		result = append(result, predicate)
	}
	return result, nil
}
