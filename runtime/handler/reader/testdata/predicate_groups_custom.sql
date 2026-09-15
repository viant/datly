#package('example.com/app/orders/read')
#import('jwt', 'github.com/viant/scy/auth/jwt')
#import('security', 'example.com/app/orders/security')
#setting($_ = $input_type('OrderSearchInput'))
#setting($_ = $output_type('OrderSearchOutput'))
#setting($_ = $case_format('lc'))
#setting($_ = $route('/orders/search', 'GET'))
#setting($_ = $connector('main'))
#define($_ = $JWT<string,*jwt.Claims>(header/Authorization).Required().WithCodec('JwtClaim').WithStatusCode(401).WithErrorMessage('authentication required').WithPredicate(0, 'handler', 'security.OrderScope'))
#define($_ = $TenantID<int>(tenant/tenant).Required().WithPredicate(0, 'equal', 'r', 'tenant_id'))
#define($_ = $WorkspaceID<int>(workspace/workspace).Required().WithPredicate(0, 'equal', 'r', 'workspace_id'))
#define($_ = $Search<string>(query/q).Optional().WithPredicate(1, 'contains', 'r', 'name').WithPredicate(1, 'contains', 'r', 'reference'))
#define($_ = $Minimum<int>(query/min).Optional().WithPredicate(2, 'greater_or_equal', 'r', 'total'))
#define($_ = $Maximum<int>(query/max).Optional().WithPredicate(2, 'less_or_equal', 'r', 'total'))
#define($_ = $Reference<string>(query/ref).Optional().WithPredicate(2, 'handler', 'security.ExactReference'))
#define($_ = $Data<[]*Order>(output/view))
SELECT orders.*, type(orders, 'Order')
FROM (
    SELECT r.id, r.tenant_id, r.workspace_id, r.name, r.reference, r.total
    FROM purchase_orders r
    ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("WHERE")}
    ORDER BY r.id
) orders
