package ssm

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"github.com/pkg/errors"
	access2 "github.com/viant/datly/v0/secret/access"
)

type service struct {
	*ssm.SSM
}

func (s *service) getParameters(name string, withDecryption bool) (*ssm.Parameter, error) {
	output, err := s.GetParameter(&ssm.GetParameterInput{
		Name:           aws.String(name),
		WithDecryption: &withDecryption,
	})
	if err != nil {
		return nil, err
	}
	return output.Parameter, nil
}

func (s *service) Access(ctx context.Context, request *access2.Request) ([]byte, error) {
	output, err := s.GetParameterWithContext(ctx, &ssm.GetParameterInput{
		Name:           aws.String(request.Parameter),
		WithDecryption: aws.Bool(true),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get secrets for '%v'", request.Parameter)
	}
	var value []byte
	if output.Parameter != nil {
		value = []byte(*output.Parameter.Value)
	}
	return value, nil
}

//New creates a new ssm access service
func New() (access2.Service, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	return &service{
		SSM: ssm.New(sess),
	}, nil
}
