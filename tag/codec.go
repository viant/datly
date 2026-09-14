package tag

import (
	"fmt"
	"strings"

	tagly "github.com/viant/tagly/tags"
)

const CodecName = "codec"

type Codec struct {
	Name       string
	Body       string
	Arguments  []string
	OutputType string
}

func ParseCodec(value string) (*Codec, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	name, values := tagly.Values(value).Name()
	name, err := decodeScalarValue(strings.TrimSpace(name))
	if err != nil {
		return nil, err
	}
	result := &Codec{Name: name}
	err = values.MatchPairs(func(key, value string) error {
		rawKey := strings.TrimSpace(key)
		rawValue := strings.TrimSpace(value)
		decodedKey, keyErr := decodeScalarValue(rawKey)
		switch strings.ToLower(decodedKey) {
		case "name":
			if keyErr != nil {
				break
			}
			result.Name, err = decodeScalarValue(rawValue)
			return err
		case "body":
			if keyErr != nil {
				break
			}
			result.Body, err = decodeScalarValue(rawValue)
			return err
		case "outputtype":
			if keyErr != nil {
				break
			}
			result.OutputType, err = decodeScalarValue(rawValue)
			return err
		}
		argument := rawKey
		if rawValue != "" {
			argument += "=" + rawValue
		}
		argument, err = decodeScalarValue(argument)
		if err != nil {
			return err
		}
		result.Arguments = append(result.Arguments, argument)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result.Body == "" {
		result.Body = result.Name
	}
	return result, nil
}

// Value formats codec metadata using the same grammar accepted by ParseCodec.
func (c Codec) Value() (string, error) {
	body := strings.TrimSpace(c.Body)
	if body == "" {
		body = strings.TrimSpace(c.Name)
	}
	if body == "" {
		return "", fmt.Errorf("codec body is required")
	}
	values := []string{encodeScalarValue(body)}
	if outputType := strings.TrimSpace(c.OutputType); outputType != "" {
		values = append(values, "outputType="+encodeScalarValue(outputType))
	}
	for _, argument := range c.Arguments {
		values = append(values, encodeScalarValue(argument))
	}
	return strings.Join(values, ","), nil
}
