package output

import (
	"errors"
	"net/http"
	"strings"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	dexec "github.com/viant/datly/exec"
	dauth "github.com/viant/datly/runtime/auth"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

// bindingBranch is bounded invocation-local provenance, never output discovery.
// Wrapper text is intentionally not part of owner or public-message authority.
type bindingBranch struct {
	owner    *bindly.BindingError
	input    *bindinput.Error
	children []*bindingBranch
	group    bool
}

func projectBindingErrors(cause error, status int) (result []*legacy.Error, ok bool) {
	// Foreign Unwrap implementations are not trusted to be well behaved.
	defer func() {
		if recover() != nil {
			result = nil
			ok = false
		}
	}()
	remaining := 128
	branch, valid := readBindingBranch(cause, &remaining)
	if !valid {
		return nil, false
	}
	return branch.project(status)
}

func readBindingBranch(err error, remaining *int) (*bindingBranch, bool) {
	if err == nil {
		return &bindingBranch{}, true
	}
	*remaining--
	if *remaining < 0 {
		return nil, false
	}
	n := &bindingBranch{}
	switch e := err.(type) {
	case modern.BodyError:
		return nil, false // genuine public bodies belong to the existing gateway.
	case *bindly.BindingError:
		if e == nil || strings.TrimSpace(e.Name) == "" {
			return nil, false
		}
		n.owner = e
	case *bindly.ResolutionGroupError:
		if e == nil {
			return nil, false
		}
		n.group = true
		for _, failure := range e.Failures() {
			child, ok := readBindingBranch(failure, remaining)
			if !ok || !child.hasOwner() {
				return nil, false
			}
			n.children = append(n.children, child)
		}
		return n, len(n.children) > 0
	case *bindinput.Error:
		if e == nil {
			return nil, false
		}
		n.input = e
	case *dauth.VerificationFailure:
		// The opaque verifier may carry private diagnostics and inherited status
		// authority. Never unwrap, classify or format it for new Errors.
		return nil, false
	default:
		// Unknown status-coder authority can alter native classification. Do not
		// invoke it or replace its inherited code with a diagnostic default.
		if _, ok := err.(interface{ StatusCode() int }); ok {
			return nil, false
		}
		if _, ok := err.(interface{ As(any) bool }); ok {
			return nil, false
		}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		n.group = true
		for _, cause := range joined.Unwrap() {
			if cause == nil {
				continue
			}
			child, ok := readBindingBranch(cause, remaining)
			if !ok || !child.hasOwner() {
				return nil, false
			}
			n.children = append(n.children, child)
		}
		return n, len(n.children) > 0
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		cause := wrapped.Unwrap()
		if cause != nil {
			child, ok := readBindingBranch(cause, remaining)
			if !ok {
				return nil, false
			}
			n.children = []*bindingBranch{child}
		}
	}
	return n, true
}

// A joined/group branch must carry complete owner authority even when an
// enclosing explicit code would prevent descent into its details.
func (n *bindingBranch) hasOwner() bool {
	if n.owner != nil {
		return true
	}
	if n.group {
		for _, child := range n.children {
			if !child.hasOwner() {
				return false
			}
		}
		return len(n.children) > 0
	}
	return len(n.children) == 1 && n.input == nil && n.children[0].hasOwner()
}

func (n *bindingBranch) code(fallback int) int {
	if n.owner != nil && n.owner.Code != 0 {
		return n.owner.Code
	}
	if n.input != nil && n.input.Code != 0 {
		return n.input.Code
	}
	for _, child := range n.children {
		if code := child.code(0); code != 0 {
			return code
		}
	}
	if n.input != nil {
		return http.StatusBadRequest
	}
	return fallback
}

func (n *bindingBranch) project(status int) ([]*legacy.Error, bool) {
	if n.owner != nil {
		code := n.code(status)
		if code < http.StatusBadRequest || code > 599 {
			return nil, false
		}
		message, ok := n.publicText(code)
		if !ok {
			return nil, false
		}
		entry := &legacy.Error{Parameter: n.owner.Name, Code: code, Err: n.owner.Unwrap(), Message: message}
		if n.owner.Code == 0 && len(n.children) > 0 {
			details, ok := n.children[0].owners(code)
			if !ok {
				return nil, false
			}
			if len(details) > 0 {
				entry.Object = details
			}
		}
		return []*legacy.Error{entry}, true
	}
	return n.owners(status)
}

// owners permits diagnostic leaves only below a real owner. At the public root
// every joined branch needs its own owner; mixed/partial trees are discarded.
func (n *bindingBranch) owners(status int) ([]*legacy.Error, bool) {
	if n.owner != nil {
		return n.project(status)
	}
	if n.group {
		var entries []*legacy.Error
		for _, child := range n.children {
			next, ok := child.project(status)
			if !ok || len(next) == 0 {
				return nil, false
			}
			entries = append(entries, next...)
		}
		return entries, len(entries) > 0
	}
	if len(n.children) == 1 && n.input == nil {
		return n.children[0].owners(status)
	}
	if n.input != nil || len(n.children) == 0 {
		return nil, true
	}
	return nil, false
}

func (n *bindingBranch) publicText(status int) (string, bool) {
	if status >= http.StatusInternalServerError {
		return http.StatusText(status), true
	}
	safe, ok := n.safeNativeError()
	if !ok {
		return "", false
	}
	return dexec.ErrorMessage(safe, status), true
}

// safeNativeError reuses native public formatting on fresh provenance copies.
// It never exposes an opaque diagnostic or wrapper prefix through a new entry.
func (n *bindingBranch) safeNativeError() (error, bool) {
	if n.owner != nil {
		clone := *n.owner
		clone.Err = nil
		clone.Cause = nil
		if len(n.children) > 0 {
			cause, ok := n.children[0].safeNativeError()
			if !ok {
				return nil, false
			}
			clone.Cause = cause
		}
		// A template that needs private interpolation has no safe projection here.
		// This recognizes existing binding template syntax, never English wording.
		if strings.Contains(clone.Message, "${error}") {
			return nil, false
		}
		if clone.Message == "" && clone.Cause == nil {
			return nil, false
		}
		return &clone, true
	}
	if n.input != nil {
		return &bindinput.Error{Code: n.code(http.StatusBadRequest)}, true
	}

	if n.group {
		var causes []error
		for _, child := range n.children {
			cause, ok := child.safeNativeError()
			if !ok {
				return nil, false
			}
			causes = append(causes, cause)
		}
		return errors.Join(causes...), len(causes) > 0
	}
	if len(n.children) == 1 {
		return n.children[0].safeNativeError()
	}
	return nil, false
}
