package access

import "context"

//Service represents secret access service
type Service interface {
	//Accesses secrets
	Access(ctx context.Context, request *Request) ([]byte, error)
}

