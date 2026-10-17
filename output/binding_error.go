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
	owner                 *bindly.BindingError
	requiredAbsence       string
	input                 *bindinput.Error
	children              []*bindingBranch
	group                 bool
	nominal               *legacy.Error
	hasNominal            bool
	verifier, hasVerifier bool
}

func projectBindingErrors(cause error, status int, readerView string) (result []*legacy.Error, message string, ok bool) {
	// Foreign Unwrap implementations are not trusted to be well behaved.
	defer func() {
		if recover() != nil {
			result = nil
			message = ""
			ok = false
		}
	}()
	remaining := 128
	branch, valid := readBindingBranch(cause, &remaining)
	if !valid {
		return nil, "", false
	}
	if branch.hasVerifier {
		return branch.projectOpaqueVerifier(status, readerView)
	}
	if branch.hasNominal {
		return branch.projectNominal(status, readerView)
	}
	result, ok = branch.project(status)
	return result, "", ok
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
		n.requiredAbsence, _ = e.RequiredAbsenceMessage()
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
			n.hasNominal = n.hasNominal || child.hasNominal
			n.hasVerifier = n.hasVerifier || child.hasVerifier
		}
		return n, len(n.children) > 0
	case *legacy.Error:
		// Only these public fields are authority. Err and Object are never traversed.
		if e == nil || e.Code < 400 || e.Code > 499 || strings.TrimSpace(e.Message) == "" || e.View != "" || e.Parameter != "" || e.Object != nil {
			return nil, false
		}
		n.nominal, n.hasNominal = e, true
		return n, true
	case *bindinput.Error:
		if e == nil {
			return nil, false
		}
		n.input = e
	case *dauth.VerificationFailure:
		// Retain only the exact opaque boundary; never inspect private authority.
		if e == nil {
			return nil, false
		}
		n.verifier, n.hasVerifier = true, true
		return n, true
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
			n.hasNominal = n.hasNominal || child.hasNominal
			n.hasVerifier = n.hasVerifier || child.hasVerifier
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
			n.hasNominal = child.hasNominal
			n.hasVerifier = child.hasVerifier
		}
	}
	return n, true
}

// projectOpaqueVerifier uses only one explicit transport owner directly above
// the terminal recorded by the same bounded parse. No inherited diagnostic
// status, text, nested owner or verifier method is authority for this exception.
func (n *bindingBranch) projectOpaqueVerifier(status int, readerView string) ([]*legacy.Error, string, bool) {
	if readerView == "" || status < 400 || status >= 500 {
		return nil, "", false
	}
	// Transparent wrappers may precede the entire chain, never bridge owners.
	for n != nil && n.owner == nil {
		if n.group || n.input != nil || n.hasNominal || n.verifier || len(n.children) != 1 {
			return nil, "", false
		}
		n = n.children[0]
	}
	var ancestors []*bindly.BindingError
	for n != nil && n.owner != nil && n.owner.Kind == "component" {
		owner := n.owner
		if owner.Code != 0 || owner.Message != "" || n.group || n.input != nil || len(n.children) != 1 || n.children[0].owner == nil {
			return nil, "", false
		}
		ancestors = append(ancestors, owner)
		n = n.children[0]
	}
	if n == nil || n.owner == nil || n.group || n.input != nil || n.hasNominal || len(n.children) != 1 || !n.children[0].verifier {
		return nil, "", false
	}
	owner := n.owner
	if owner.Code < 400 || owner.Code >= 500 || owner.Code != status || strings.Contains(owner.Message, "${") {
		return nil, "", false
	}
	switch owner.Kind {
	case "header", "query", "path", "cookie", "form", "body":
	default:
		return nil, "", false
	}
	message := http.StatusText(owner.Code)
	if strings.TrimSpace(owner.Message) != "" {
		message = owner.Message
	}
	if strings.TrimSpace(message) == "" {
		return nil, "", false
	}
	cause := owner.Cause
	if cause == nil {
		cause = owner.Err
	}
	details := []*legacy.Error{{Parameter: owner.Name, Code: owner.Code, Message: message, Err: cause}}
	for i := len(ancestors) - 1; i >= 0; i-- {
		ancestor := ancestors[i]
		details = []*legacy.Error{{Parameter: ancestor.Name, Code: status, Message: message, Err: ancestor.Unwrap(), Object: details}}
	}
	details[0].View = readerView
	return details, message, true
}

// projectNominal admits only one already-read linear owner chain ending in the
// exact nominal public business record. No diagnostic method or private cause
// is called here; all other shapes retain the original plain envelope.
func (n *bindingBranch) projectNominal(status int, readerView string) ([]*legacy.Error, string, bool) {
	if readerView == "" {
		return nil, "", false
	}
	var owners []*bindly.BindingError
	for n != nil {
		if n.group || n.input != nil {
			return nil, "", false
		}
		if n.owner != nil {
			if n.owner.Message != "" || (n.owner.Code != 0 && n.owner.Code != status) {
				return nil, "", false
			}
			owners = append(owners, n.owner)
		}
		if n.nominal != nil {
			if len(owners) == 0 || n.nominal.Code != status {
				return nil, "", false
			}
			message := n.nominal.Message
			var details []*legacy.Error
			for i := len(owners) - 1; i >= 0; i-- {
				owner := owners[i]
				entry := &legacy.Error{Parameter: owner.Name, Code: status, Message: message, Err: owner.Unwrap()}
				if owner.Code == 0 && len(details) > 0 {
					entry.Object = details
				}
				details = []*legacy.Error{entry}
			}
			details[0].View = readerView
			return details, message, true
		}
		if len(n.children) != 1 {
			return nil, "", false
		}
		n = n.children[0]
	}
	return nil, "", false
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
		if clone.Message == "" {
			if text, ok := n.requiredText(); ok {
				clone.Message = text
			}
		}
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

// requiredText carries only the exact absence owner's safe literal through a
// linear binding chain. Authored messages and groups retain their existing policy.
func (n *bindingBranch) requiredText() (string, bool) {
	if n == nil || n.group || n.input != nil || n.owner == nil || n.owner.Message != "" {
		return "", false
	}
	if n.requiredAbsence != "" {
		return n.requiredAbsence, true
	}
	if len(n.children) == 1 {
		return n.children[0].requiredText()
	}
	return "", false
}

// readerBindingSource retains source identity without changing error status,
// public bodies, or transport encoding. Only a compiled native reader issues it.
type readerBindingSource struct {
	cause error
	view  string
}

func (e *readerBindingSource) Error() string { return e.cause.Error() }
func (e *readerBindingSource) Unwrap() error { return e.cause }

// sourceReaderView accepts only transparent, single-cause forwarding. A new
// binding owner or joined outcome must not borrow a descendant's source name.
func sourceReaderView(cause error) (view string) {
	defer func() {
		if recover() != nil {
			view = ""
		}
	}()
	for remaining := 128; cause != nil && remaining > 0; remaining-- {
		if source, ok := cause.(*readerBindingSource); ok {
			return source.view
		}
		switch cause.(type) {
		case *bindly.BindingError, *bindinput.Error, *bindly.ResolutionGroupError,
			interface{ Unwrap() []error }, interface{ StatusCode() int }, interface{ As(any) bool }:
			return ""
		}
		next, ok := cause.(interface{ Unwrap() error })
		if !ok {
			return ""
		}
		cause = next.Unwrap()
	}
	return ""
}

// PreserveReaderBindingSource is used by scoped invocation after completion.
// It records only validated binding failures; nominal business errors remain exact.
func (p *Plan) PreserveReaderBindingSource(cause error) (result error) {
	result = cause
	defer func() {
		if recover() != nil {
			result = cause
		}
	}()
	if cause == nil || p == nil || p.errorEnvelope == nil || p.errorEnvelope.readerView == "" {
		return cause
	}
	if _, ok := cause.(*legacy.Error); ok {
		return cause
	}
	// Validate the entire graph before status traversal: errors.As alone is
	// unbounded and permits application-defined As/Unwrap implementations.
	remaining := 128
	branch, valid := readBindingBranch(cause, &remaining)
	if !valid || branch.hasNominal || branch.hasVerifier {
		return cause
	}
	status := branch.code(0)
	if status < 400 || status > 499 {
		return cause
	}
	if _, ok := branch.project(status); !ok {
		return cause
	}
	return &readerBindingSource{cause: cause, view: p.errorEnvelope.readerView}
}
