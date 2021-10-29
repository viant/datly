package data

import "context"

//Visit represent an object visitor
type Visit func(ctx *context.Context, value interface{}) (bool, error)
