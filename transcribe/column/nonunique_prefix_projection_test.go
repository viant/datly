package column

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

// The controlled connector returns native MySQL metadata, while using an owned
// SQLite connection beneath it. No MySQL service or schema is needed.
func TestPhysicalSourceConstraintsNonuniquePrefixProjection(t *testing.T) {
	ctx := context.Background()
	r, c, v := constraintMySQLFixture(t)
	c.rows["indexes"].values[0][5] = "1" // MySQL NON_UNIQUE, not inferred uniqueness.
	c.rows["members"].values[0][7] = int64(1024)
	v.Source.SQL = "SELECT r.id,r.a FROM records r"
	v.Columns = append(v.Columns, &spec.Column{Name: "a", Source: "a"})
	sealed, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Len(t, sealed.Projection, 2)
	require.Len(t, sealed.Indexes, 1)
	require.False(t, sealed.Indexes[0].Unique)
	require.Equal(t, "1", sealed.Indexes[0].RawUnique)
	require.Equal(t, int64(1024), *sealed.Indexes[0].Members[0].PrefixLength)
	require.Equal(t, "A", sealed.Indexes[0].Members[0].Collation)
	again, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.Equal(t, sealed, again)
	encoded, err := json.Marshal(sealed)
	require.NoError(t, err)
	var roundTrip PhysicalSourceConstraints
	require.NoError(t, json.Unmarshal(encoded, &roundTrip))
	require.Equal(t, sealed, roundTrip, "prefix/direction remain serialized physical evidence")
	for _, prefix := range []int64{512, 0} {
		c.rows["members"].values[0][7] = prefix
		fresh, err := r.PhysicalSourceConstraints(ctx, "main", v)
		require.NoError(t, err)
		require.NotEqual(t, sealed, fresh, "prefix drift cannot compare equal")
		require.Equal(t, int64(1024), *sealed.Indexes[0].Members[0].PrefixLength, "captured facts are detached")
	}
	c.rows["members"].values[0][7] = int64(1024)
	c.rows["members"].values[0][6] = "D"
	descending, err := r.PhysicalSourceConstraints(ctx, "main", v)
	require.NoError(t, err)
	require.NotEqual(t, sealed, descending, "direction drift cannot compare equal")
	require.Equal(t, "D", descending.Indexes[0].Members[0].Collation)
	// Prefix admission supplies no authority for an invalid projection.
	v.Source.SQL = "SELECT r.a FROM records r"
	v.Columns = v.Columns[1:]
	_, err = r.PhysicalSourceConstraints(ctx, "main", v)
	require.ErrorContains(t, err, "one projected physical primary key is required")
}

func TestPhysicalSourceConstraintsNonuniquePrefixProjectionGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*constraintMetadataConnector)
	}{
		{"unique", func(c *constraintMetadataConnector) { c.rows["indexes"].values[0][5] = "0" }},
		{"primary", func(c *constraintMetadataConnector) {
			c.rows["indexes"].values[0][4] = "PRIMARY"
			c.rows["members"].values[0][3] = "PRIMARY"
		}},
		{"unknown uniqueness", func(c *constraintMetadataConnector) { c.rows["indexes"].values[0][5] = "?" }},
		{"negative prefix", func(c *constraintMetadataConnector) { c.rows["members"].values[0][7] = int64(-1) }},
		{"unknown prefix", func(c *constraintMetadataConnector) { c.rows["members"].values[0][7] = nil }},
		{"unknown direction", func(c *constraintMetadataConnector) { c.rows["members"].values[0][6] = "?" }},
		{"missing direction", func(c *constraintMetadataConnector) { c.rows["members"].values[0][6] = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, c, v := constraintMySQLFixture(t)
			c.rows["indexes"].values[0][5] = "1"
			c.rows["members"].values[0][7] = int64(1024)
			tc.mutate(c)
			_, err := r.PhysicalSourceConstraints(context.Background(), "main", v)
			require.Error(t, err)
		})
	}
}
