# Cube compose

Cube compose is an opt-in companion endpoint for a groupable Datly cube. Enable
it in DQL without introducing a separate predicate for every operator/column:

```dql
#set( $_ = $cube())
#set( $_ = $cubeCompose(true))
```

Datly then exposes `POST <cube-route>/cube/compose` as an MCP tool by default.
The tool contract contains two typed cube frames plus caller-selected wrapper
SQL. Each frame accepts the source cube's filters. `cube2.inherit: true` copies
filters omitted from cube 2 from cube 1, which is useful for advertiser,
publisher, site, and other shared scope filters.

The SQL is a constrained projection over exactly two trusted macros:
`$CubeSQL1 AS t1` and `$CubeSQL2 AS t2`. The caller—typically an LLM—chooses
the comparison expression, conditions, ordering, and limit. Datly parses and
validates that SQL against the cube's exposed dimensions and measures, binds
literals as query parameters, prepares both cube queries through the normal
Datly path, and executes the final projection through the regular Datly view
reader. Arbitrary sources, subqueries, wildcards, unqualified fields, and
caller placeholders are rejected. SQL is regenerated from the validated AST,
so caller comments and original SQL text never reach the database.

The wrapper SQL defines a completely new projection on every request. Datly
creates its row type with `reflect.StructOf`, reads a collection of that type,
and does not register or cache a view for the shape. Consequently, dictionaries
and outer-view enrichment configured on the original cube are not applied to
composed output. Select any required display values explicitly as exposed cube
dimensions instead. The response contract keeps the dynamic collection in
`data any`; `columns` describes the validated projection.

Only `JOIN` and `LEFT JOIN` are portable parts of the comparison contract. To
find rows missing from the current frame, place the previous period in `cube1`
and current period in `cube2`; swap frames to find newly appearing rows. This
avoids depending on vendor-specific `FULL OUTER JOIN` support.

Example: top five ad-order spend drops today versus the same elapsed portion of
yesterday, including orders with no spend today:

```json
{
  "cube1": {
    "align": "elapsed",
    "filters": {
      "period": "yesterday",
      "advertiserId": 29
    }
  },
  "cube2": {
    "inherit": true,
    "filters": {
      "period": "today"
    }
  },
  "sql": "SELECT t1.ad_order_id, COALESCE(t2.total_spend, 0) AS current_spend, t1.total_spend AS previous_spend, t1.total_spend - COALESCE(t2.total_spend, 0) AS spend_drop FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.ad_order_id = t2.ad_order_id WHERE COALESCE(t2.total_spend, 0) < t1.total_spend ORDER BY spend_drop DESC LIMIT 5"
}
```

`align: "elapsed"` is delivered to source predicates through
`contract.CubeComposeFrameContext`. A period-aware predicate can therefore
truncate yesterday, last week, or last month to the same elapsed duration as
the incomplete current period. Datly does not guess a source's business-time
semantics.

The same contract supports spend, bids, CTR, VTR, win rate, bid rate, CPA, and
other exposed measures; the SQL expression—not a column-specific predicate
family—defines equality, thresholds, deltas, ratios, and top-N ranking. The
function allowlist is deliberately vendor-neutral; use standard expressions
such as `value / NULLIF(denominator, 0)` for safe ratios.
