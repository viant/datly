package shape

import "errors"

var (
	ErrNilSource             = errors.New("shape: source was nil")
	ErrNilDQL                = errors.New("shape: dql was empty")
	ErrScannerNotConfigured  = errors.New("shape: scanner was not configured")
	ErrPlannerNotConfigured  = errors.New("shape: planner was not configured")
	ErrLoaderNotConfigured   = errors.New("shape: loader was not configured")
	ErrCompilerNotConfigured = errors.New("shape: compiler was not configured")
)
