package codec

import (
	"context"
	"encoding/base64"
	"errors"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strings"
)

// ExtractBasicAuth extracts the username and password from the Authorization header.
func ExtractBasicAuth(authHeader string) (string, string, error) {
	const prefix = "Basic "
	// Check if the header starts with "Basic "
	if !strings.HasPrefix(authHeader, prefix) {
		return "", "", errors.New("invalid authorization header format")
	}
	// Remove the "Basic " prefix
	encodedCredentials := strings.TrimPrefix(authHeader, prefix)

	// Decode the base64 encoded credentials
	decodedBytes, err := base64.StdEncoding.DecodeString(encodedCredentials)
	if err != nil {
		return "", "", errors.New("failed to decode base64 credentials")
	}

	// Split the decoded string into username and password
	decodedCredentials := string(decodedBytes)
	parts := strings.SplitN(decodedCredentials, ":", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid credential format")
	}

	return parts[0], parts[1], nil
}

const (
	KeyBasicAuthSubject = "BasicAuthSubject"
	KeyBasicAuthSecret  = "BasicAuthSecret"
)

type BasicAuthSubject struct {
}

func (i *BasicAuthSubject) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(""), nil
}

func (i *BasicAuthSubject) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	subject, _, err := ExtractBasicAuth(raw.(string))
	if err != nil {
		return "", err
	}
	return subject, nil
}

type BasicAuthSecret struct {
}

func (i *BasicAuthSecret) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(""), nil
}

func (i *BasicAuthSecret) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	_, secret, err := ExtractBasicAuth(raw.(string))
	if err != nil {
		return "", err
	}
	return secret, nil
}
