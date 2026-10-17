// Package readfailure carries native read provenance without exposing SQL text.
package readfailure

import (
	"net/http"

	"github.com/viant/bindly"
	sqlxread "github.com/viant/sqlx/io/read"
	modern "github.com/viant/xdatly/response"
)

type failure struct {
	cause       error
	view, stage string
}

func (e *failure) Error() string { return e.cause.Error() }
func (e *failure) Unwrap() error { return e.cause }

// Capture is called immediately after SQLX QueryAll, before application callbacks
// can replace the outcome. Only SQLX's own terminal source marker grants authority.
func Capture(err error, view string) error {
	source, ok := err.(*sqlxread.SourceError)
	if !ok || source == nil || view == "" || source.Unwrap() == nil {
		return err
	}
	stage := source.Stage()
	if stage != "prepare" && stage != "query" {
		return err
	}
	return &failure{cause: err, view: view, stage: stage}
}

// Message admits the complete bounded graph. Additional causes or binding/public
// response ownership cannot be silently discarded in favor of a SQL diagnostic.
func Message(err error, status int, view string) (message string, matched bool) {
	// Error graphs are application-controlled. Unsupported/panicking unwrap
	// implementations must retain the existing plain-envelope fallback.
	defer func() {
		if recover() != nil {
			message, matched = "", false
		}
	}()
	if status != http.StatusInternalServerError || view == "" {
		return "", false
	}
	budget := 64
	var selected *failure
	var visit func(error) bool
	visit = func(e error) bool {
		budget--
		if e == nil || budget < 0 {
			return false
		}
		if f, ok := e.(*failure); ok {
			if f == nil || f.view != view || (selected != nil && selected != f) {
				return false
			}
			selected = f
			return true
		}
		switch e.(type) {
		case *bindly.BindingError, modern.BodyError:
			return false
		}
		if _, ok := e.(interface{ StatusCode() int }); ok {
			return false
		}
		if _, ok := e.(interface{ As(any) bool }); ok {
			return false
		}
		if _, ok := e.(interface{ Is(error) bool }); ok {
			return false
		}
		if group, ok := e.(interface{ Unwrap() []error }); ok {
			children := group.Unwrap()
			if len(children) == 0 {
				return false
			}
			for _, child := range children {
				if !visit(child) {
					return false
				}
			}
			return true
		}
		if one, ok := e.(interface{ Unwrap() error }); ok {
			return visit(one.Unwrap())
		}
		return false
	}
	if !visit(err) || selected == nil {
		return "", false
	}
	return "database error occurred while fetching Data for view " + selected.view + ": SQL " + selected.stage + " failed", true
}
