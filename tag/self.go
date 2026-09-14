package tag

import (
	"fmt"
	"strings"

	tagly "github.com/viant/tagly/tags"
)

const SelfName = "self"

type SelfReference struct {
	Child  string
	Parent string
}

// Value formats self-reference metadata using ParseSelf's grammar.
func (s SelfReference) Value() (string, error) {
	child := strings.TrimSpace(s.Child)
	parent := strings.TrimSpace(s.Parent)
	if child == "" || parent == "" {
		return "", fmt.Errorf("self-reference requires child and parent fields")
	}
	if strings.ContainsAny(child+parent, ",=\"`\n\r") {
		return "", fmt.Errorf("self-reference contains an unsupported delimiter")
	}
	return "child=" + child + ",parent=" + parent, nil
}

func ParseSelf(value string) (*SelfReference, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	result := &SelfReference{}
	_, values := tagly.Values(value).Name()
	err := values.MatchPairs(func(key, value string) error {
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "child":
			result.Child = strings.TrimSpace(value)
		case "parent":
			result.Parent = strings.TrimSpace(value)
		default:
			return fmt.Errorf("unsupported self-reference option %q", key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result.Child == "" || result.Parent == "" {
		return nil, fmt.Errorf("self-reference requires child and parent fields")
	}
	return result, nil
}
