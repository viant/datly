package ast

type Options struct {
	Lang                 string
	StateName            string
	CallNotifier         func(callExpr *CallExpr) (Expression, error)
	AssignNotifier       func(assign *Assign) (Expression, error)
	SliceItemNotifier    func(value, set *Ident) error
	WithoutBusinessLogic bool
	OnIfNotifier         func(value *Condition) (Expression, error)
	WithLowerCaseIdent   bool
}
