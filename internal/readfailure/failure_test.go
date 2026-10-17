package readfailure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/bindly"
	sqlxread "github.com/viant/sqlx/io/read"
)

type cycle struct{}

func (*cycle) Error() string   { panic("private error text read") }
func (e *cycle) Unwrap() error { return e }

type coded struct{ error }

func (e *coded) Unwrap() error { return e.error }
func (*coded) StatusCode() int { return 503 }

func TestActualPrepareProvenance(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := sqlxread.New(context.Background(), db, "SELECT secret FROM private_credentials", func() any { return &struct{ Secret string }{} }, sqlxread.WithSourceErrorProvenance())
	if err != nil {
		t.Fatal(err)
	}
	raw := r.QueryAll(context.Background(), func(any) error { return nil })
	cause := Capture(raw, "Records")
	if cause == raw {
		t.Fatal("actual prepare marker missing")
	}
	for _, tc := range []struct {
		name  string
		cause error
		want  bool
	}{
		{"actual", cause, true}, {"wrapped", fmt.Errorf("private wrapper: %w", cause), true},
		{"duplicate", errors.Join(cause, cause), true}, {"distinct", errors.Join(cause, Capture(raw, "Records")), false},
		{"extra", errors.Join(cause, errors.New("private cleanup")), false}, {"cancel", errors.Join(cause, context.Canceled), false},
		{"binding", &bindly.BindingError{Name: "Auth", Kind: "component", Cause: cause}, false},
		{"status", &coded{cause}, false}, {"cycle", &cycle{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, ok := Message(tc.cause, 500, "Records")
			if ok != tc.want {
				t.Fatalf("matched %v want %v", ok, tc.want)
			}
			if ok && (text != "database error occurred while fetching Data for view Records: SQL prepare failed" || strings.Contains(text, "secret")) {
				t.Fatal("public diagnostic changed", text)
			}
		})
	}
	if _, ok := Message(cause, 500, "Other"); ok {
		t.Fatal("wrong view admitted")
	}
	if _, ok := Message(cause, 403, "Records"); ok {
		t.Fatal("wrong status admitted")
	}
	if !errors.Is(cause, raw) {
		t.Fatal("private cause lost")
	}
}
