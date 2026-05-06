SELECT
    foo.foo_id,
    SUM(foo.bar_metric) AS total_metric
FROM
    foo_table foo
    ${predicate.Builder().CombineOr($predicate.FilterGroup(0, "AND")).Build("WHERE")}
GROUP BY
    foo.foo_id
${predicate.Builder().CombineOr($predicate.FilterGroup(1, "AND")).Build("HAVING")}
