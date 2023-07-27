/* {
    "URI":"custom-predicates",
    "Method": "GET",
    "Include": ["./templates.yaml"]
   } */


#set($_ = $ParamState<?>(state/).WithPredicate(0, "price_range", "t", "PRICE"))
#set($_ = $PriceMin<int>(query/priceMin).WithTag('velty:"names=PriceMin|ValueMin"').Optional())
#set($_ = $ID<int>(query/id).Optional())
#set($_ = $PriceMax<int>(query/priceMax).WithTag('velty:"names=PriceMax|ValueMax"').Optional())

SELECT bar.*
FROM (SELECT *
      FROM BAR t
      WHERE 1 = 1 ${predicate.Builder().CombineOr(
        $predicate.Ctx(0, "AND"),
        $predicate.Ctx(1, "OR" )
      ).Build("AND")}
    ) bar