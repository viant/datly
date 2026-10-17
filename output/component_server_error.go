package output

import (
	"context"
	"net/http"
	"reflect"

	"github.com/viant/bindly"
	dexec "github.com/viant/datly/exec"
	legacy "github.com/viant/xdatly/handler/response"
	modern "github.com/viant/xdatly/response"
)

// maskedComponentServerError preserves an owned, masked component failure when
// completion also returns the exact retained cause. All branches must be proved;
// identical messages/types never establish identity and no diagnostics are read.
func maskedComponentServerError(cause error, status int, view string) ([]*legacy.Error, bool) {
	if status != http.StatusInternalServerError || view == "" {
		return nil, false
	}
	budget := 128
	var branches []error
	var flatten func(error) bool
	flatten = func(e error) bool {
		budget--
		if e == nil || budget < 0 {
			return false
		}
		if _, public := e.(modern.BodyError); public {
			return false
		}
		if many, ok := e.(interface{ Unwrap() []error }); ok {
			if _, coded := e.(interface{ StatusCode() int }); coded {
				return false
			}
			if _, synthetic := e.(interface{ As(any) bool }); synthetic {
				return false
			}
			if _, synthetic := e.(interface{ Is(error) bool }); synthetic {
				return false
			}
			if _, panicCause := e.(*dexec.PanicError); panicCause {
				return false
			}
			children := many.Unwrap()
			if len(children) == 0 {
				return false
			}
			for _, child := range children {
				if !flatten(child) {
					return false
				}
			}
			return true
		}
		branches = append(branches, e)
		return true
	}
	if !flatten(cause) {
		return nil, false
	}
	var owner *bindly.BindingError
	var nominal *legacy.Error
	owned := -1
	// Only a component binding chain may grant the nominal error ownership.
	for i, branch := range branches {
		e := branch
		var first *bindly.BindingError
		for e != nil {
			budget--
			if budget < 0 {
				return nil, false
			}
			if b, ok := e.(*bindly.BindingError); ok {
				if b == nil || b.Kind != "component" || b.Name == "" || (b.Code != 0 && b.Code != status) || b.Message != "" {
					break
				}
				if first == nil {
					first = b
				}
				e = b.Unwrap()
				continue
			}
			if n, ok := e.(*legacy.Error); ok && n != nil && first != nil && n.Code == status && n.Err != nil {
				if owner != nil {
					return nil, false
				}
				owner, nominal, owned = first, n, i
			}
			break
		}
	}
	if owner == nil {
		return nil, false
	}
	// Collect comparable pointer identities from one complete retained chain.
	// Value errors are allowed as leaves but cannot establish shared identity.
	identities := map[error]bool{}
	edges := map[error]error{}
	walk := func(e error, collect bool) (bool, bool) {
		seen := map[error]bool{}
		matched := false
		for e != nil {
			budget--
			if budget < 0 {
				return false, false
			}
			if e == context.Canceled || e == context.DeadlineExceeded {
				return false, false
			}
			switch e.(type) {
			case modern.BodyError, *dexec.PanicError:
				return false, false
			}
			if _, group := e.(interface{ Unwrap() []error }); group {
				return false, false
			}
			if b, ok := e.(*bindly.BindingError); ok {
				if b == nil || (b.Code != 0 && b.Code != status) {
					return false, false
				}
			} else {
				if _, coded := e.(interface{ StatusCode() int }); coded {
					return false, false
				}
				if _, synthetic := e.(interface{ As(any) bool }); synthetic {
					return false, false
				}
				if _, synthetic := e.(interface{ Is(error) bool }); synthetic {
					return false, false
				}
			}
			typ := reflect.TypeOf(e)
			if typ.Kind() == reflect.Pointer && typ.Comparable() {
				if seen[e] {
					return false, false
				}
				seen[e] = true
				if collect {
					identities[e] = true
				} else if identities[e] {
					matched = true
				}
			}
			if one, ok := e.(interface{ Unwrap() error }); ok {
				next := one.Unwrap()
				if typ.Kind() == reflect.Pointer && typ.Comparable() {
					if prior, exists := edges[e]; exists {
						if !sameServerCauseEdge(prior, next) {
							return false, false
						}
					} else {
						edges[e] = next
					}
				}
				e = next
			} else {
				return true, matched
			}
		}
		return false, false
	}
	if valid, _ := walk(nominal.Err, true); !valid {
		return nil, false
	}
	for i, branch := range branches {
		if i == owned {
			continue
		}
		if valid, shared := walk(branch, false); !valid || !shared {
			return nil, false
		}
	}
	return []*legacy.Error{{Parameter: owner.Name, View: view, Code: status, Message: http.StatusText(status), Err: owner.Unwrap()}}, true
}

// Compare retained successors without custom Is/As hooks or error messages.
func sameServerCauseEdge(a, b error) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return reflect.TypeOf(a).Comparable() && reflect.TypeOf(b).Comparable() && a == b
}
