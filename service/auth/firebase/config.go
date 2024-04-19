package firebase

import (
	firebase "firebase.google.com/go/v4"
	"github.com/viant/scy"
)

type Config struct {
	Resource *scy.Resource
	Config   *firebase.Config
}
