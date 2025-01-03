package codec

import (
	"context"
	"fmt"
	sfirebase "github.com/viant/scy/auth/firebase"

	"github.com/viant/datly/service/auth/firebase"
	"github.com/viant/scy/auth"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"google.golang.org/api/googleapi"
	"reflect"
	"sync"
)

const (
	KeyFirebaseAuth = "FirebaseAuth"
)

type FirebaseAuth struct {
	codecConfig  *codec.Config
	firebaseAuth *firebase.Service
	sync.Mutex
}

func (e *FirebaseAuth) New(codecConfig *codec.Config, options ...codec.Option) (codec.Instance, error) {
	if len(codecConfig.Args) < 2 {
		return nil, fmt.Errorf("at least 2 arguments are required")
	}
	var err error
	if e.firebaseAuth == nil {
		if e.firebaseAuth, err = firebase.New(context.Background(), &sfirebase.Config{}); err != nil {
			return nil, err
		}
	}
	return &FirebaseAuth{codecConfig: codecConfig, firebaseAuth: e.firebaseAuth}, nil
}

func (e *FirebaseAuth) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&auth.Token{}), nil
}

func (e *FirebaseAuth) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	if raw == nil {
		return nil, nil
	}
	aType := structology.NewStateType(reflect.TypeOf(raw))
	state := aType.WithValue(raw)
	username, err := state.String(e.codecConfig.Args[0])
	if err != nil {
		return nil, err
	}
	password, err := state.String(e.codecConfig.Args[1])
	if err != nil {
		return nil, err
	}

	token, err := e.firebaseAuth.BasicAuth(ctx, username, password)
	if err != nil {
		return nil, obfuscateErrorMessage(err)
	}
	return token, err
}

func obfuscateErrorMessage(err error) error {
	switch actual := err.(type) {
	case *googleapi.Error:
		if len(actual.Errors) > 0 {
			if actual.Errors[0].Message == "EMAIL_NOT_FOUND" || actual.Errors[0].Reason == "invalid" {
				return fmt.Errorf("invalid credentials")
			}
		}
	}
	return err
}

func NewFirebaseAuth(firebaseAuth *firebase.Service) codec.Factory {
	return &FirebaseAuth{firebaseAuth: firebaseAuth}
}
