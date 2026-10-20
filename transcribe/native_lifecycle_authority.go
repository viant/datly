package transcribe

import (
	"github.com/viant/datly/transcribe/internal/bodygraph"
	"strings"
)

// selectNativeComponentLifecycle runs after graph and column refinement.
// Reuse that graph: embedded resources can establish auxiliary intent only
// during refinement, after authored output relations have also been attached.
// Package declarations still own their identity/types, but cannot silently
// restore factory execution when this source selects a native batch lifecycle.
func (c *componentCompilation) selectNativeComponentLifecycle() {
	if c.authored == nil || c.authored.RootView == nil || c.result == nil || c.result.Source == nil || strings.TrimSpace(c.result.Source.Text) == "" {
		return
	}
	component := c.result.Component
	root := component.RootView
	if root == nil || !root.Auxiliary || strings.TrimSpace(root.EntityHooks) == "" || root.SelfReference != nil || len(root.Relations) != 0 && !bodygraph.AllAuxiliary(root) {
		return // Physical row/child hooks retain ordinary handler inheritance.
	}
	count := 0
	for _, route := range c.authored.Routes {
		if route == nil {
			continue
		}
		if strings.TrimSpace(route.Handler) != "" {
			return // Explicit authored factories retain their execution authority.
		}
		switch strings.ToUpper(strings.TrimSpace(route.Method)) {
		case "POST", "PUT", "PATCH", "DELETE":
			count++
		default:
			return
		}
	}
	if count == 0 {
		return
	}
	for _, route := range component.Routes {
		if route != nil {
			route.Handler = ""
		}
	}
	// compileViews owns a detached Source. Do not change the caller's linked
	// factory, and keep the linked input/output reflect types for type discovery.
	c.result.Source.LinkedHandler = nil
}
