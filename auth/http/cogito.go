package http

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cognitoidentityprovider"
)

type Cognito struct {
	Region   string
	ClientID string
	client   *cognitoidentityprovider.CognitoIdentityProvider
}

func NewCognito(region, clientID string) (*Cognito, error) {
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		return nil, err
	}
	return &Cognito{
		client:   cognitoidentityprovider.New(sess),
		ClientID: clientID,
	}, nil

}
