SELECT id, name FROM active_records
WHERE $criteria.In("tenant_id", $TenantIDs)
#if($IncludeArchive)
UNION ALL
SELECT id, name FROM archived_records
WHERE $criteria.In("tenant_id", $TenantIDs)
#end
ORDER BY id, name
