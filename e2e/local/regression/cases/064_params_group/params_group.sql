/* {
    "URI":"params-group",
    "Method": "GET",
    "Include": ["./templates.yaml"]
   } */


#set($_ = $PriceRange<?>(object/).WithPredicate(0, "custom_range", "t", "PRICE").Optional())
#set($_ = $PriceMin<int>(query/priceMin).Of('PriceRange').WithTag('velty:"names=PriceMin|ValueMin"').Optional())
#set($_ = $PriceMax<int>(query/priceMax).Of('PriceRange').WithTag('velty:"names=PriceMax|ValueMax"').Optional())

#set($_ = $IDRange<?>(object/).WithPredicate(0, "custom_range", "t", "ID").Optional())
#set($_ = $IDMin<int>(query/idMin).Of('IDRange').WithTag('velty:"names=IDMin|ValueMin"').Optional())
#set($_ = $IDMax<int>(query/idMax).Of('IDRange').WithTag('velty:"names=IDMax|ValueMax"').Optional())

#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT bar.*
FROM (SELECT *
      FROM BAR t
      WHERE 1 = 1 ${predicate.Builder().CombineOr(
        $predicate.FilterGroup(0, "AND"),
        $predicate.FilterGroup(1, "OR" )
      ).Build("AND")}
     ) bar