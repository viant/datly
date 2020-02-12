package storage

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/afsc/gs"
	"github.com/viant/datly/secret/access"
	"google.golang.org/api/cloudkms/v1"
	"google.golang.org/api/option"
	"io/ioutil"
)

type service struct {
	fs afs.Service
}

func (s *service) downloadBase64(ctx context.Context, URL string) (string, error) {
	reader, err := s.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return "", err
	}
	defer func() { _ = reader.Close() }()
	data, err := ioutil.ReadAll(reader)
	_, err = base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		return string(data), nil
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

//Decrypt decrypts plainText with supplied key
func (s *service) Access(ctx context.Context, request *access.Request) ([]byte, error) {
	plainText, err := s.downloadBase64(ctx, request.URL)
	if err != nil {
		return nil, err
	}
	if url.Scheme(request.URL, "") == gs.Scheme {
		return s.decryptWithGCPKMS(ctx, request, plainText)
	}
	//TODO aws kms
	return []byte(plainText), nil
}

func (s *service) decryptWithGCPKMS(ctx context.Context, request *access.Request, plainText string) ([]byte, error) {
	kmsService, err := cloudkms.NewService(ctx, option.WithScopes(cloudkms.CloudPlatformScope, cloudkms.CloudkmsScope))
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to create kmsService server for key %v", request.Key))
	}
	service := cloudkms.NewProjectsLocationsKeyRingsCryptoKeysService(kmsService)
	response, err := service.Decrypt(request.Key, &cloudkms.DecryptRequest{Ciphertext: plainText}).Context(ctx).Do()
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to decrypt with key %v", request.Key))
	}
	return []byte(response.Plaintext), nil
}

//New creates GCP kms service
func New(fs afs.Service) access.Service {
	return &service{fs: fs}
}
