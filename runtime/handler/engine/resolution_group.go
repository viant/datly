package engine

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/datly/internal/drainowner"
	"github.com/viant/datly/runtime/registry"
)

type resolutionGroupController struct {
	scope   *dataScope
	request Request
}
type resolutionGroupRun struct{ group drainowner.BindingGroup }

func (c resolutionGroupController) Open(ctx context.Context, plan bindly.ResolutionGroupPlan) (bindly.ResolutionGroupRun, error) {
	if c.scope == nil || !c.request.BufferedComponentCalls || !drainowner.IsBindingGroupProvider(c.request.Components) || c.request.BoundInput != nil || c.request.Replay != nil {
		return nil, fmt.Errorf("resolution groups require canonical buffered input and native component authority")
	}
	if c.request.ResolvedInput != nil && plan.Mode() != bindly.ResolutionGroupDependencies {
		return nil, fmt.Errorf("reader seeds require dependency-mode resolution groups")
	}
	if err := validateGroupedInputRequest(c.request); err != nil {
		return nil, err
	}
	if plan.Mode() == bindly.ResolutionGroupDependencies {
		return c.openReadOnly(plan)
	}
	if ReadOnlyDependency(ctx) {
		return nil, RejectReadOnlyDependency(ctx, "legacy resolution groups cannot run inside a read-only group")
	}
	fields := map[string]registry.InputField{}
	for _, field := range c.request.Input.Fields() {
		fields[field.Path()] = field
	}
	barriers := plan.After()
	if len(barriers) != 2 {
		return nil, fmt.Errorf("resolution group requires JWT and body barriers")
	}
	hasJWT, hasBody := false, false
	for _, path := range barriers {
		field, ok := fields[path]
		if !ok {
			return nil, fmt.Errorf("resolution group barrier %s is not canonical", path)
		}
		binding := field.Binding()
		if field.VerifiesJWT() && binding.Location.Kind == "header" {
			hasJWT = true
		}
		if binding.Location.Kind == "body" && binding.Required != nil && *binding.Required && field.DestinationType().Kind() == reflect.Pointer && field.DestinationType().Elem().Kind() == reflect.Struct {
			hasBody = true
		}
	}
	if !hasJWT || !hasBody {
		return nil, fmt.Errorf("resolution group requires verified JWT and required body success")
	}
	members := plan.Members()
	if len(members) != 5 {
		return nil, fmt.Errorf("resolution group requires the reviewed finite five-member closure")
	}
	paths, reads, independent := 0, 0, 0
	descriptors := make([]drainowner.BindingGroupMember, 0, len(members))
	for _, member := range members {
		field, ok := fields[member.Path]
		if !ok {
			return nil, fmt.Errorf("resolution group member %s is not canonical", member.Path)
		}
		descriptor := drainowner.BindingGroupMember{Path: member.Path}
		if member.Location.Kind == "path" {
			paths++
			if len(member.ResolutionGroup.DependsOn) != 0 {
				return nil, fmt.Errorf("group root path cannot have prerequisites")
			}
		} else {
			reads++
			target, ok := field.Dependency()
			if !ok {
				return nil, fmt.Errorf("resolution group %s has no canonical component dependency", member.Path)
			}
			descriptor.Target = target.String()
			if len(member.ResolutionGroup.DependsOn) == 0 {
				independent++
			} else if len(member.ResolutionGroup.DependsOn) != 1 {
				return nil, fmt.Errorf("group reader %s requires one canonical path", member.Path)
			}
			for _, path := range member.ResolutionGroup.DependsOn {
				dependency, ok := fields[path]
				if !ok {
					return nil, fmt.Errorf("group prerequisite %s not found", path)
				}
				binding := dependency.Binding()
				if binding.Location.Kind != "path" || binding.Transformer != nil || (binding.SourceType != nil && binding.SourceType != dependency.DestinationType()) {
					return nil, fmt.Errorf("group prerequisite %s must be an unadapted path", path)
				}
				descriptor.Inputs = append(descriptor.Inputs, drainowner.BindingGroupInput{Kind: binding.Location.Kind, In: binding.Location.In, Type: dependency.DestinationType()})
			}
		}
		descriptors = append(descriptors, descriptor)
	}
	if paths != 1 || reads != 4 || independent != 1 {
		return nil, fmt.Errorf("resolution group requires path, independent Auth read, and three path-dependent native readers")
	}
	root := c.scope
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	issuer := root.nativeIssuerLocked()
	root.mu.Unlock()
	group, err := drainowner.OpenBindingGroup(issuer, descriptors)
	if err != nil {
		return nil, err
	}
	return &resolutionGroupRun{group: group}, nil
}
func (r *resolutionGroupRun) Enter(ctx context.Context, path string) (context.Context, error) {
	return r.group.Enter(ctx, path)
}
func (r *resolutionGroupRun) Observe(_ context.Context, outcome bindly.ResolutionOutcome) error {
	failure := outcome.Failure()
	if failure == nil {
		if outcome.Cause() != nil {
			_, err := drainowner.RecordBindingGroupFailure(r.group, outcome.Path(), outcome.Cause(), true)
			return err
		}
		return nil
	}
	_, err := drainowner.RecordBindingGroupFailure(r.group, outcome.Path(), failure, outcome.Terminal())
	return err
}
func (r *resolutionGroupRun) Close(context.Context) error { return r.group.Close() }

func validateGroupedInputRequest(request Request) error {
	grouped := false
	for _, field := range request.Input.Fields() {
		if field.Binding().ResolutionGroup == nil {
			continue
		}
		grouped = true
		if request.BoundInput != nil || request.Replay != nil || request.IndependentChildTransactions {
			return fmt.Errorf("resolution groups require fresh canonical input binding")
		}
	}
	if grouped && request.ResolvedInput != nil {
		if !request.CanonicalReader {
			return fmt.Errorf("resolution group seeds require a canonical native reader")
		}
		return ValidateGroupedSeedPaths(request.Input, request.ResolvedPaths)
	}
	return nil
}
