package exec

import xhandler "github.com/viant/xdatly/handler"

// InputSeed retains source compatibility with the public Xdatly contract.
type InputSeed = xhandler.InputSeed

func WithInput(input any) *InputSeed { return xhandler.WithInput(input) }
