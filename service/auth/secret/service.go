package secret

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/scy"
	"os"
	"path"
	"strings"
)

const (
	gcpSecret                = "gcp"
	gcpDefaultCredentialsKey = "GOOGLE_APPLICATION_CREDENTIALS"
)

type Service struct {
	secrets *scy.Service
	fs      afs.Service
}

func (s *Service) Apply(ctx context.Context, resource *Resource) error {
	switch strings.ToLower(string(resource.Kind)) {
	case gcpSecret:
		if os.Getenv(gcpDefaultCredentialsKey) != "" {
			return fmt.Errorf("unable to set " + gcpSecret + " secret " + gcpDefaultCredentialsKey + " alrady set")
		}
		secret, err := s.secrets.Load(ctx, &resource.Resource)
		if err != nil {
			return err
		}
		data := secret.String()
		if secret.Name == "" {
			secret.Name = gcpSecret
		}
		location := path.Join(os.TempDir(), secret.Name)
		if err = s.fs.Upload(ctx, location, file.DefaultFileOsMode, strings.NewReader(data)); err != nil {
			return err
		}
		os.Setenv(gcpDefaultCredentialsKey, location)
	default:
		return fmt.Errorf("not supported yet")
	}
	return nil
}

// New creates a service
func New() *Service {
	return &Service{
		secrets: scy.New(),
		fs:      afs.New(),
	}
}
