#setting($_ = $route('/spend','GET'))
#setting($_ = $cube())
#setting($_ = $cubeCompose(true))
#define($_ = $Tenant<string>(query/tenant).WithTag('json:"tenant"').WithPredicate(0,'equal','s','tenant'))
#define($_ = $Channel<string>(query/channel).WithTag('json:"channel"').WithPredicate(0,'handler','%s/spend.ChannelPredicate'))
SELECT groupable(s), s.account_id, SUM(s.amount) AS total_spend,
CAST(s.account_id AS int), tag(s.account_id,'groupable:"true"'), CAST(s.total_spend AS float64)
FROM spend s
WHERE s.version <= %d
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}
GROUP BY s.account_id
