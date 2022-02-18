package data

//Visit represent an object visitor
type Visit func(ctx *Context, value *Value) (bool, error)
