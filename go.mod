module github.com/viant/datly

go 1.20

require (
	github.com/aerospike/aerospike-client-go v4.5.2+incompatible
	github.com/aws/aws-lambda-go v1.31.0
	github.com/francoispqt/gojay v1.2.13
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator v9.31.0+incompatible
	github.com/go-sql-driver/mysql v1.7.0
	github.com/goccy/go-json v0.9.11
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/google/gops v0.3.23
	github.com/google/uuid v1.3.0
	github.com/jessevdk/go-flags v1.5.0
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lib/pq v1.10.6
	github.com/mattn/go-sqlite3 v1.14.16
	github.com/onsi/gomega v1.20.2 // indirect
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.8.4
	github.com/viant/afs v1.24.2
	github.com/viant/afsc v1.9.0
	github.com/viant/assertly v0.9.1-0.20220620174148-bab013f93a60
	github.com/viant/bigquery v0.2.1
	github.com/viant/cloudless v1.8.1
	github.com/viant/dsc v0.16.2 // indirect
	github.com/viant/dsunit v0.10.8
	github.com/viant/dyndb v0.1.4-0.20221214043424-27654ab6ed9c
	github.com/viant/gmetric v0.2.7-0.20220508155136-c2e3c95db446
	github.com/viant/godiff v0.4.1
	github.com/viant/parsly v0.2.0
	github.com/viant/pgo v0.10.4-0.20230801151735-5700d39f8114
	github.com/viant/scy v0.6.0
	github.com/viant/sqlx v0.9.1-0.20230703200209-9a495fb0b0de
	github.com/viant/structql v0.4.1
	github.com/viant/toolbox v0.34.6-0.20221112031702-3e7cdde7f888
	github.com/viant/velty v0.2.1-0.20230803192842-d7c215d7aaac
	github.com/viant/xdatly/types/custom v0.0.0-20230309034540-231985618fc7
	github.com/viant/xreflect v0.3.2-0.20230723180303-1f248c397c5c
	github.com/viant/xunsafe v0.9.0
	golang.org/x/mod v0.11.0
	golang.org/x/oauth2 v0.7.0 // indirect
	google.golang.org/api v0.114.0
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/viant/govalidator v0.2.2-0.20230629211659-f2a3f0659b0b
	github.com/viant/sqlparser v0.5.1-0.20230802023432-9d7400ae861a
	golang.org/x/crypto v0.10.0 // indirect
)

require (
	github.com/aws/aws-sdk-go v1.44.12
	github.com/aws/aws-sdk-go-v2/config v1.18.3
	github.com/aws/aws-sdk-go-v2/service/s3 v1.33.1
	github.com/viant/structology v0.3.1-0.20230807231757-9c77ad06c186
	github.com/viant/xdatly v0.3.1-0.20230804224640-c6697394a9ac
	github.com/viant/xdatly/extension v0.0.0-20230323215422-3e5c3147f0e6
	github.com/viant/xdatly/handler v0.0.0-20230713223438-282037388a67
	github.com/viant/xdatly/types/core v0.0.0-20230619231115-e622dd6aff79
	golang.org/x/tools v0.10.0
)

require (
	cloud.google.com/go v0.110.0 // indirect
	cloud.google.com/go/compute v1.19.0 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.13.0 // indirect
	cloud.google.com/go/secretmanager v1.10.0 // indirect
	cloud.google.com/go/storage v1.29.0 // indirect
	github.com/aws/aws-sdk-go-v2 v1.18.0 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.13.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.10.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.12.19 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.1.33 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.4.27 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.3.26 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.0.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.17.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.13.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.9.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.1.28 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.7.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.9.27 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.14.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sns v1.20.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sqs v1.22.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.11.25 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.13.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.17.5 // indirect
	github.com/aws/smithy-go v1.13.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.0-20210816181553-5444fa50b93d // indirect
	github.com/go-errors/errors v1.4.2 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.8.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.0 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.1 // indirect
	github.com/lestrrat-go/jwx v1.2.25 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/nxadm/tail v1.4.8 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.9.0 // indirect
	github.com/viant/igo v0.1.0 // indirect
	github.com/yuin/gopher-lua v0.0.0-20221210110428-332342483e3f // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/net v0.11.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
	golang.org/x/sys v0.9.0 // indirect
	golang.org/x/term v0.9.0 // indirect
	golang.org/x/text v0.10.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	google.golang.org/grpc v1.54.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
