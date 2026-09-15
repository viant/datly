package exec

import (
	"errors"
	"fmt"
	"github.com/viant/bindly"
	"testing"
)

func TestErrorMessagePublicPolicy(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		want   string
	}{
		{errors.New("missing required database table PRIVATE"), 500, "Internal Server Error"},
		{fmt.Errorf("private wrapper: %w", &bindly.BindingError{Code: 500, Message: "authored message", Cause: errors.New("PRIVATE")}), 500, "authored message"},
		{&bindly.BindingError{Code: 401, Message: "denied"}, 401, "denied"},
	} {
		if got := ErrorMessage(tc.err, tc.status); got != tc.want {
			t.Fatalf("got=%q want=%q", got, tc.want)
		}
	}
}
