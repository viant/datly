package decl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse_ExtractsDeclarations(t *testing.T) {
	sql := `
SELECT ad.*,
       cast(ad.ACTIVE as bool),
       cast(ad.CHANNELS AS '[]string'),
       tag(ad.CHANNELS, 'sqlx:"-"'),
       set_limit(ad, 25),
       allow_nulls(ad)
FROM CI_AD_ORDER ad`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 5)

	require.Equal(t, KindCast, decls[0].Kind)
	require.Equal(t, "ad.ACTIVE", decls[0].Target)
	require.Equal(t, "bool", decls[0].DataType)

	require.Equal(t, KindCast, decls[1].Kind)
	require.Equal(t, "[]string", decls[1].DataType)

	require.Equal(t, KindTag, decls[2].Kind)
	require.Equal(t, `sqlx:"-"`, decls[2].Tag)

	require.Equal(t, KindSetLimit, decls[3].Kind)
	require.Equal(t, "25", decls[3].Limit)

	require.Equal(t, KindAllowNulls, decls[4].Kind)
	require.Equal(t, "ad", decls[4].Target)
}

func TestParse_IgnoresQuotedAndCommented(t *testing.T) {
	sql := `
SELECT
  'cast(a as int)',
  "tag(x,'json')",
  /* set_limit(a,1), allow_nulls(a) */
  cast(t.ACTIVE as bool)
FROM T t`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 1)
	require.Equal(t, KindCast, decls[0].Kind)
}

func TestParse_SupportsNestedArgs(t *testing.T) {
	sql := `SELECT tag(ad.NAME, concat('a,', upper('b'))), set_limit(ad, ifnull(25, 10)) FROM T ad`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 2)
	require.Equal(t, KindTag, decls[0].Kind)
	require.Equal(t, "concat('a,', upper('b'))", decls[0].Tag)
	require.Equal(t, "ifnull(25, 10)", decls[1].Limit)
}

func TestParse_AllowsWhitespaceBetweenNameAndParen(t *testing.T) {
	sql := `SELECT cast   (ad.ACTIVE as bool), allow_nulls ( ad ) FROM T ad`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 2)
	require.Equal(t, KindCast, decls[0].Kind)
	require.Equal(t, KindAllowNulls, decls[1].Kind)
}

func TestParse_InvalidInputProducesNoError(t *testing.T) {
	sql := `SELECT cast ad.ACTIVE as bool FROM T`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 0)
}

func TestParse_ExtractsExtendedSettings(t *testing.T) {
	sql := `
SELECT x.*,
       set_partitioner(x, 'pkg.Part', 7),
       use_connector(x, 'bq_mdp'),
       match_strategy(x, 'read_all'),
       compress_above_size(1024),
       batch_size(x, 20000),
       relational_concurrency(x, 10),
       publish_parent(x),
       cardinality(x, 'One')
FROM T x`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 8)

	require.Equal(t, KindSetPartitioner, decls[0].Kind)
	require.Equal(t, "pkg.Part", decls[0].Partition)
	require.Equal(t, "7", decls[0].Value)

	require.Equal(t, KindUseConnector, decls[1].Kind)
	require.Equal(t, "bq_mdp", decls[1].Connector)

	require.Equal(t, KindMatchStrategy, decls[2].Kind)
	require.Equal(t, "read_all", decls[2].Strategy)

	require.Equal(t, KindCompressAboveSize, decls[3].Kind)
	require.Equal(t, "1024", decls[3].Size)

	require.Equal(t, KindBatchSize, decls[4].Kind)
	require.Equal(t, "20000", decls[4].Value)

	require.Equal(t, KindRelationalConcurrency, decls[5].Kind)
	require.Equal(t, "10", decls[5].Value)

	require.Equal(t, KindPublishParent, decls[6].Kind)
	require.Equal(t, "x", decls[6].Target)

	require.Equal(t, KindCardinality, decls[7].Kind)
	require.Equal(t, "One", decls[7].Value)
}

func TestParse_ExtractsPackageAndImport(t *testing.T) {
	sql := `
#set($_ = $package('mdp/performance'))
#set($_ = $import('perf', 'github.com/acme/mdp/performance'))
#set($_ = $import('github.com/acme/shared/types'))
SELECT x.*
FROM T x`
	decls, err := Parse(sql)
	require.NoError(t, err)
	require.Len(t, decls, 3)

	require.Equal(t, KindPackage, decls[0].Kind)
	require.Equal(t, "mdp/performance", decls[0].Package)

	require.Equal(t, KindImport, decls[1].Kind)
	require.Equal(t, "perf", decls[1].Alias)
	require.Equal(t, "github.com/acme/mdp/performance", decls[1].Package)

	require.Equal(t, KindImport, decls[2].Kind)
	require.Equal(t, "", decls[2].Alias)
	require.Equal(t, "github.com/acme/shared/types", decls[2].Package)
}
