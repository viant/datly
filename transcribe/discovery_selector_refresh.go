package transcribe

import (
	"fmt"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/transcribe/internal/link"
	"github.com/viant/datly/transcribe/internal/sourcecontract"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
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

// preparedSelectorSource is the final handoff of the same prepared-source
// admission used for provisional discovery. Metadata alone cannot mint it.
func preparedSelectorSource(result *Result) (*sourcecontract.Selector, error) {
	if result == nil || result.Source == nil || result.Component == nil || result.Component.SelectorCompanion == nil {
		return nil, nil
	}
	source := result.Source
	if source.PackageComponent == nil || strings.TrimSpace(source.Text) == "" {
		return nil, nil
	}
	if source.preparation == nil || !source.preparation.matches(source) || source.PackageComponent.SelectorCompanion != result.Component.SelectorCompanion {
		return nil, fmt.Errorf("prepared selector source lineage mismatch")
	}
	if result.preparedSelector == nil {
		return nil, fmt.Errorf("prepared selector compilation evidence missing")
	}
	if err := result.preparedSelector.Validate(result.Component); err != nil {
		return nil, err
	}
	return result.preparedSelector, nil
}

// capturePreparedSelectorSource runs only after the package compiler finishes
// native source normalization. Reuse that product instead of replaying parsing,
// predicate compilation and linked metadata merging at generation time.
func capturePreparedSelectorSource(result *Result, input, output *x.Type) error {
	if result == nil || result.Source == nil || result.Component == nil || result.Component.SelectorCompanion == nil || strings.TrimSpace(result.Source.Text) == "" {
		return nil
	}
	source := result.Source
	if source.PackageComponent == nil || source.preparation == nil || !source.preparation.matches(source) || source.PackageComponent.SelectorCompanion != result.Component.SelectorCompanion {
		return fmt.Errorf("prepared selector source lineage mismatch")
	}
	types, err := typecatalog.NewResolver(source.Types, typecatalog.PackageAuthority, result.TypeContext)
	if err != nil {
		return err
	}
	result.preparedSelector, err = sourcecontract.NewSelector(source.PackageComponent, result.Component, input, output, source.Types, types)
	return err
}
