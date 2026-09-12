package locator

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingBody struct{}

func (failingBody) Read([]byte) (int, error) {
	return 0, errors.New("failed to read request body")
}

func (failingBody) Close() error {
	return nil
}

func TestBodyValueReturnsRequestBodyReadError(t *testing.T) {
	request := httptest.NewRequest("POST", "http://localhost/test", nil)
	request.Body = failingBody{}

	aLocator, err := NewBody(
		WithRequest(request),
		WithBodyType(reflect.TypeOf(struct{}{})),
		WithUnmarshal(func([]byte, interface{}) error { return nil }),
	)
	require.NoError(t, err)

	value, ok, err := aLocator.Value(context.Background(), reflect.TypeOf(struct{}{}), "")
	require.Nil(t, value)
	require.False(t, ok)
	require.EqualError(t, err, "failed to read request body")
}

func TestReadRequestBodyRejectsNilRequest(t *testing.T) {
	data, err := readRequestBody(nil)
	require.Nil(t, data)
	require.EqualError(t, err, "request was empty")
}
