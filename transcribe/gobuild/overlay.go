package gobuild

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (c *Context) absolutePath(name string) (string, error) {
	if !filepath.IsAbs(name) {
		name = filepath.Join(c.Dir, name)
	}
	return filepath.Abs(name)
}

// GOFLAGS quotes whole arguments, without shell escaping or expansion.
func splitGoFlags(value string) ([]string, error) {
	var result []string
	for {
		value = strings.TrimLeft(value, " \t\r\n")
		if value == "" {
			return result, nil
		}
		if quote := value[0]; quote == '\'' || quote == '"' {
			end := strings.IndexByte(value[1:], quote)
			if end < 0 {
				return nil, fmt.Errorf("unterminated GOFLAGS quote %c", quote)
			}
			result = append(result, value[1:end+1])
			value = value[end+2:]
		} else {
			end := strings.IndexAny(value, " \t\r\n")
			if end < 0 {
				return append(result, value), nil
			}
			result = append(result, value[:end])
			value = value[end:]
		}
	}
}

func (c *Context) inheritedOverlay() (map[string]string, error) {
	data, err := c.run("env", "GOFLAGS")
	if err != nil {
		return nil, err
	}
	flags, err := splitGoFlags(string(data))
	if err != nil {
		return nil, err
	}
	name := ""
	for _, flag := range flags {
		if value, ok := strings.CutPrefix(flag, "-overlay="); ok {
			name = value
		}
	}
	replace := map[string]string{}
	if name == "" {
		return replace, nil
	}
	name, err = c.absolutePath(name)
	if err != nil {
		return nil, err
	}
	data, err = os.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("read inherited Go overlay: %w", err)
	}
	var overlay struct{ Replace map[string]string }
	if err = json.Unmarshal(data, &overlay); err != nil {
		return nil, fmt.Errorf("decode inherited Go overlay: %w", err)
	}
	for from, to := range overlay.Replace {
		from, err = c.absolutePath(from)
		if err != nil {
			return nil, err
		}
		if to != "" {
			to, err = c.absolutePath(to)
			if err != nil {
				return nil, err
			}
		}
		replace[from] = to
		physical, err := physicalPath(from)
		if err != nil {
			return nil, err
		}
		replace[physical] = to
	}
	return replace, nil
}
