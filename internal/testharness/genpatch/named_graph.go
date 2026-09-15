package genpatch

// NamedGraphDQL keeps the client-facing graph on named outer views. The inner
// table marker makes kind a lookup, independently of its one-cardinality join.
const NamedGraphDQL = PackageDirective + `
#setting($_ = $route('/orders','PATCH'))
SELECT orders.*, items.*, kind.*
FROM (SELECT o.* FROM ORDERS o) orders
LEFT JOIN (SELECT i.* FROM ITEMS i) items ON items.ORDER_ID = orders.ID
LEFT JOIN (SELECT k.* FROM (ORDER_KINDS) k) kind ON kind.ID = orders.KIND_ID AND 1=1
`
