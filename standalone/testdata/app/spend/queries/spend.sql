SELECT s.account_id AS customer_id, s.region AS region_code,
SUM(s.amount) AS spend_total, COUNT(*) AS order_count
FROM report_spend s
WHERE 1 = 1
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}
GROUP BY s.account_id, s.region
