module github.com/viant/datly

go 1.23.8

require (
	github.com/aerospike/aerospike-client-go v4.5.2+incompatible
	github.com/aws/aws-lambda-go v1.31.0
	github.com/francoispqt/gojay v1.2.13
	github.com/go-sql-driver/mysql v1.7.0
	github.com/goccy/go-json v0.10.5
	github.com/golang-jwt/jwt/v4 v4.5.1 // indirect
	github.com/google/gops v0.3.23
	github.com/google/uuid v1.6.0
	github.com/jessevdk/go-flags v1.5.0
	github.com/lib/pq v1.10.6
	github.com/mattn/go-sqlite3 v1.14.16
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.10.0
	github.com/viant/afs v1.26.2
	github.com/viant/afsc v1.9.1
	github.com/viant/assertly v0.9.1-0.20220620174148-bab013f93a60
	github.com/viant/bigquery v0.4.1
	github.com/viant/cloudless v1.12.0
	github.com/viant/dsc v0.16.2 // indirect
	github.com/viant/dsunit v0.10.8
	github.com/viant/dyndb v0.1.4-0.20221214043424-27654ab6ed9c
	github.com/viant/gmetric v0.3.1-0.20230405233616-cc90deee60c4
	github.com/viant/godiff v0.4.1
	github.com/viant/parsly v0.3.3-0.20240717150634-e1afaedb691b
	github.com/viant/pgo v0.11.0
	github.com/viant/scy v0.24.0
	github.com/viant/sqlx v0.16.6
	github.com/viant/structql v0.5.2
	github.com/viant/toolbox v0.36.0
	github.com/viant/velty v0.2.1-0.20230927172116-ba56497b5c85
	github.com/viant/xreflect v0.7.3
	github.com/viant/xunsafe v0.10.3
	golang.org/x/mod v0.17.0
	golang.org/x/oauth2 v0.30.0
	google.golang.org/api v0.174.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/viant/govalidator v0.3.1
	github.com/viant/sqlparser v0.8.1
)

require (
	github.com/viant/aerospike v0.2.11-0.20241108195857-ed524b97800d
	github.com/viant/firebase v0.1.1
	github.com/viant/jsonrpc v0.5.4
	github.com/viant/mcp v0.2.9
	github.com/viant/mcp-protocol v0.3.4
	github.com/viant/structology v0.6.1
	github.com/viant/tagly v0.2.2
	github.com/viant/xdatly v0.5.4-0.20250418144853-029d9a05ae20
	github.com/viant/xdatly/extension v0.0.0-20231013204918-ecf3c2edf259
	github.com/viant/xdatly/handler v0.0.0-20250418144853-029d9a05ae20
	github.com/viant/xdatly/types/core v0.0.0-20250307183722-8c84fc717b52
	github.com/viant/xdatly/types/custom v0.0.0-20240801144911-4c2bfca4c23a
	github.com/viant/xlsy v0.3.1
	github.com/viant/xmlify v0.1.1
	golang.org/x/net v0.34.0
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d
	modernc.org/sqlite v1.18.1
)

require (
	cloud.google.com/go v0.112.1 // indirect
	cloud.google.com/go/auth v0.2.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.0 // indirect
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	cloud.google.com/go/firestore v1.15.0 // indirect
	cloud.google.com/go/iam v1.1.7 // indirect
	cloud.google.com/go/longrunning v0.5.5 // indirect
	cloud.google.com/go/secretmanager v1.11.5 // indirect
	cloud.google.com/go/storage v1.40.0 // indirect
	firebase.google.com/go v3.13.0+incompatible // indirect
	firebase.google.com/go/v4 v4.14.0 // indirect
	github.com/MicahParks/keyfunc v1.9.0 // indirect
	github.com/aerospike/aerospike-client-go/v6 v6.15.1 // indirect
	github.com/aws/aws-sdk-go v1.51.23 // indirect
	github.com/aws/aws-sdk-go-v2 v1.30.3 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.27.11 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.26 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.15 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.17.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.11.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.11.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.31.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.34.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.22.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.26.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.30.3 // indirect
	github.com/aws/smithy-go v1.20.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.2.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/s2a-go v0.1.7 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.2 // indirect
	github.com/googleapis/gax-go/v2 v2.12.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kballard/go-shellquote v0.0.0-20180428030007-95032a82bc51 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/jwx v1.2.29 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/mazznoer/csscolorparser v0.1.3 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20200410134404-eec4a21b6bb0 // indirect
	github.com/richardlehane/mscfb v1.0.4 // indirect
	github.com/richardlehane/msoleps v1.0.3 // indirect
	github.com/viant/igo v0.2.0 // indirect
	github.com/viant/x v0.3.0 // indirect
	github.com/xuri/efp v0.0.0-20230802181842-ad255f2331ca // indirect
	github.com/xuri/excelize/v2 v2.8.0 // indirect
	github.com/xuri/nfp v0.0.0-20230819163627-dc951e3ffe1a // indirect
	github.com/yuin/gopher-lua v1.1.1 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.49.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.49.0 // indirect
	go.opentelemetry.io/otel v1.24.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.opentelemetry.io/otel/trace v1.24.0 // indirect
	golang.org/x/crypto v0.38.0 // indirect
	golang.org/x/sync v0.14.0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	golang.org/x/term v0.32.0 // indirect
	golang.org/x/text v0.25.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/appengine/v2 v2.0.2 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240314234333-6e1732d8331c // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240415180920-8c6c420018be // indirect
	google.golang.org/grpc v1.63.2 // indirect
	google.golang.org/protobuf v1.33.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	lukechampine.com/uint128 v1.2.0 // indirect
	modernc.org/cc/v3 v3.36.3 // indirect
	modernc.org/ccgo/v3 v3.16.9 // indirect
	modernc.org/libc v1.17.1 // indirect
	modernc.org/mathutil v1.5.0 // indirect
	modernc.org/memory v1.2.1 // indirect
	modernc.org/opt v0.1.3 // indirect
	modernc.org/strutil v1.1.3 // indirect
	modernc.org/token v1.0.0 // indirect
)
