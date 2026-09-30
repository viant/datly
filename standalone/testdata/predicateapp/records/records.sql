SELECT id, COUNT(*) AS total FROM records
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")}
GROUP BY id
