package base

//Filter request filter
type Filter func(request *Request) (toContinue bool, err error)
