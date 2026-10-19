SELECT s.* FROM  (SELECT region,SUM(amount) AS total FROM sales GROUP BY region)  s
