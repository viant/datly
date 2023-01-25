module github.com/viant/datly/xdatly

go 1.17

require (
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/viant/scy v0.4.1
	github.com/viant/sqlx v0.4.1
	github.com/viant/structql v0.1.1-0.20221217012101-59b3abd0f9fd
	github.com/viant/xunsafe v0.8.1-0.20221217035120-48c214a9bcfc
)

require (
	cloud.google.com/go/compute v1.7.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.0-20210816181553-5444fa50b93d // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/goccy/go-json v0.9.7 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.0.0-20220520183353-fd19c99a87aa // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.0 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.1 // indirect
	github.com/lestrrat-go/jwx v1.2.25 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/viant/afs v1.16.1-0.20220601210902-dc23d64dda15 // indirect
	github.com/viant/assertly v0.9.1-0.20220308232634-4242424ccaf5 // indirect
	github.com/viant/datly/plugins v0.0.0-20230111021818-8e2b2f4673d9 // indirect
	github.com/viant/igo v0.1.0 // indirect
	github.com/viant/parsly v0.0.0-20220913214053-cb272791c00f // indirect
	github.com/viant/sqlparser v0.3.0 // indirect
	github.com/viant/toolbox v0.34.5 // indirect
	github.com/yuin/gopher-lua v0.0.0-20221210110428-332342483e3f // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/net v0.3.0 // indirect
	golang.org/x/oauth2 v0.3.0 // indirect
	golang.org/x/sys v0.3.0 // indirect
	golang.org/x/text v0.5.0 // indirect
	google.golang.org/api v0.84.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20220616135557-88e70c0c3a90 // indirect
	google.golang.org/grpc v1.47.0 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace (
	github.com/viant/datly/plugins => ../plugins
)