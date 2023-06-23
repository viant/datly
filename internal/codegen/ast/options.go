package ast

type Options struct {
	Lang                 string
	StateName            string
	CallNotifier         func(callExpr *CallExpr) (*CallExpr, error)
	WithoutBusinessLogic bool
}
