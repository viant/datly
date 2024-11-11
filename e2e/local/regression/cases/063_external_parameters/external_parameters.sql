/* {
    "URI":"external-parameters",
    "Method": "GET",
    "Include": ["./parameters.sql"]
   } */


#set( $_ = $Data<?>(output/view).WithTag('anonymous:"true"'))


SELECT vendor.*
FROM (SELECT *
      FROM VENDOR t
      WHERE 1 = 1 ${predicate.Builder().CombineOr(
        $predicate.FilterGroup(0, "AND"),
        $predicate.FilterGroup(1, "OR" )
      ).Build("AND")}
    ) vendor