package load

import "errors"

var (
	ErrEmptyViewPlan = errors.New("shape load: no views available in plan")
)
