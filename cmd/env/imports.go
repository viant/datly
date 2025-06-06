package env

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/aerospike"
	_ "github.com/viant/afs/embed"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	_ "github.com/viant/cloudless/async/mbus/aws"
	_ "github.com/viant/dyndb"
	_ "github.com/viant/firebase/firestore"
	_ "github.com/viant/firebase/realtime"
	_ "github.com/viant/scy/kms/blowfish"
	_ "github.com/viant/sqlx/metadata/product/aerospike"
	_ "github.com/viant/sqlx/metadata/product/bigquery"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	_ "github.com/viant/sqlx/metadata/product/pg"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	_ "modernc.org/sqlite"
)
