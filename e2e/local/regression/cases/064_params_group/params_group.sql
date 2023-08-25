/* {
    "URI":"params-group",
    "Method": "GET",
    "Include": ["./templates.yaml"]
   } */


#set($_ = $PriceMin<int>(query/priceMin).WithTag('velty:"names=PriceMin|ValueMin"').Optional())
#set($_ = $PriceMax<int>(query/priceMax).WithTag('velty:"names=PriceMax|ValueMax"').Optional())
#set($_ = $IDMin<int>(query/idMin).WithTag('velty:"names=IDMin|ValueMin"').Optional())
#set($_ = $IDMax<int>(query/idMax).WithTag('velty:"names=IDMax|ValueMax"').Optional())
#set($_ = $PriceRange<?>(group/PriceMin,PriceMax).WithPredicate(0, "custom_range", "t", "PRICE").Optional())
#set($_ = $IDRange<?>(group/IDMin,IDMax).WithPredicate(0, "custom_range", "t", "ID").Optional())

SELECT bar.*
FROM (SELECT *
      FROM BAR t
      WHERE 1 = 1 ${predicate.Builder().CombineOr(
        $predicate.FilterGroup(0, "AND"),
        $predicate.FilterGroup(1, "OR" )
      ).Build("AND")}
    ) bar