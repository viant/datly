package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/registry"
)

const MaxInlineState = 63 * 1024

// StateCodec bounds original name-keyed state; all field, presence, source-type
// and replay binding mechanics belong to the native canonical Bindly plan.
type StateCodec struct{ plan *bindly.ReplayPlan }

func NewStateCodec(contract *registry.RouteInputContract) (*StateCodec, error) {
	if contract == nil {
		return nil, fmt.Errorf("canonical replay input contract is required")
	}
	plan, err := contract.ReplayPlan()
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, fmt.Errorf("canonical replay plan is required")
	}
	return &StateCodec{plan: plan}, nil
}

func (c *StateCodec) Capture(input any) (*bindly.Replay, error) { return c.plan.Capture(input) }
func (c *StateCodec) Encode(input any) (string, error) {
	replay, err := c.Capture(input)
	if err != nil {
		return "", err
	}
	return c.State(replay)
}
func (c *StateCodec) State(replay *bindly.Replay) (string, error) {
	data, err := json.Marshal(replay)
	if err != nil {
		return "", err
	}
	if len(data) > MaxInlineState {
		return "", fmt.Errorf("job state exceeds original inline limit %d; state was not truncated", MaxInlineState)
	}
	return string(data), nil
}
func (c *StateCodec) Decode(state string) (*bindly.Replay, error) {
	if len(state) > MaxInlineState {
		return nil, fmt.Errorf("job state exceeds inline limit")
	}
	return c.plan.DecodeJSON([]byte(state))
}

// CaptureSources delegates raw source authority to the registered native plan.
func (c *StateCodec) CaptureSources(ctx context.Context, providers []locator.Provider) (*bindly.Replay, error) {
	return c.plan.CaptureSources(ctx, providers)
}
