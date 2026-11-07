package transcribe

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/link"
	"strings"
)

// selectorDiscoveryContract validates a linked companion against its old source
// only for an exact replacement declared by the prepared DQL. Other metadata
// retains the ordinary companion checks, including route and execution owner.
func (c *discoveryInputCompiler) selectorDiscoveryContract() (*spec.Component, error) {
	current := c.component
	if c.source == nil || c.source.PackageComponent == nil || c.source.PackageComponent.SelectorCompanion != current.SelectorCompanion || c.source.preparation == nil || !c.source.preparation.matches(c.source) || strings.TrimSpace(c.source.Text) == "" {
		return current, nil
	}
	authored, err := dql.ParsePreparedComponentSource(c.source.Scope, c.source.Name, c.source.preparation.prepared)
	if err != nil {
		return nil, err
	}
	authority := current.Clone()
	authority.Parameters = authored.Parameters
	if err = resolveQuerySelectorViews(authority); err != nil {
		return nil, err
	}
	validation := current.Clone()
	for _, old := range c.source.PackageComponent.Parameters {
		if old == nil || old.QuerySelector == nil {
			continue
		}
		retained := false
		for _, p := range current.Parameters {
			if p != nil && p.Identity() == old.Identity() {
				retained = true
				break
			}
		}
		if retained {
			continue
		}
		for _, declared := range authority.Parameters {
			if declared == nil || declared.QuerySelector == nil || declared.QuerySelector.Property != old.QuerySelector.Property || !strings.EqualFold(declared.QuerySelector.View, old.QuerySelector.View) || declared.Source.Kind != "query" || declared.Source.Name == old.Source.Name {
				continue
			}
			property, canonical := spec.SelectorPropertyForParam(declared.Name)
			if !canonical || property != declared.QuerySelector.Property {
				continue
			}
			for i, p := range current.Parameters {
				if p == nil || p.Identity() != declared.Identity() {
					continue
				}
				equal, compareErr := (&link.Contracts{PackageComponent: &spec.Component{Parameters: []*spec.Parameter{declared}}, CompiledComponent: &spec.Component{Parameters: []*spec.Parameter{p}}}).EqualParamsChecked(false)
				if compareErr != nil {
					return nil, compareErr
				}
				if !equal {
					return nil, fmt.Errorf("transcribe column: selector replacement differs from authored declaration %s", p.Name)
				}
				validation.Parameters[i] = old.Clone()
			}
		}
	}
	return validation, nil
}
