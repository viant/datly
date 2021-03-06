SELECT
 e.*,
 t.name AS event_type_name,
 t.account_id,
 a.name AS account_name
FROM events e
LEFT JOIN event_types t ON t.id = e.event_type_id
LEFT JOIN accounts a ON a.id = t.account_id
ORDER BY 1