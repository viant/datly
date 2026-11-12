package session

import (
	stderrors "errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/handler/response"
)

func TestSessionHandleParameterErrorPreservesWrappedResponseStatus(t *testing.T) {
	testCases := []struct {
		name              string
		err               error
		parameterStatus   int
		expectedStatus    int
		expectedErrorCode int
	}{
		{
			name:              "wrapped response error",
			err:               stderrors.Join(response.NewError(http.StatusUnauthorized, "unauthorized"), nil),
			expectedStatus:    http.StatusUnauthorized,
			expectedErrorCode: http.StatusUnauthorized,
		},
		{
			name: "wrapped response errors",
			err: func() error {
				previous := response.NewErrors()
				previous.Append(response.NewError(http.StatusUnauthorized, "unauthorized"))
				return stderrors.Join(previous, nil)
			}(),
			expectedStatus:    http.StatusUnauthorized,
			expectedErrorCode: http.StatusUnauthorized,
		},
		{
			name:              "parameter status overrides wrapped status",
			err:               stderrors.Join(response.NewError(http.StatusUnauthorized, "unauthorized"), nil),
			parameterStatus:   http.StatusForbidden,
			expectedStatus:    http.StatusForbidden,
			expectedErrorCode: http.StatusForbidden,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual := response.NewErrors()
			parameter := &state.Parameter{Name: "Auth", ErrorStatusCode: testCase.parameterStatus}

			(&Session{}).handleParameterError(parameter, testCase.err, actual)

			assert.Equal(t, testCase.expectedStatus, actual.StatusCode())
			require.Len(t, actual.Errors, 1)
			assert.Equal(t, testCase.expectedErrorCode, actual.Errors[0].StatusCode())
		})
	}
}
