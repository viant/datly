SELECT * FROM  (SELECT o.* FROM test_ledger o ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}
)  operations