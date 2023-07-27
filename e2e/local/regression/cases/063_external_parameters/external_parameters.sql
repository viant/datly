/* {
    "URI":"external-parameters",
    "Method": "GET",
    "Include": ["./parameters.sql"]
   } */


SELECT vendor.*
FROM (SELECT *
      FROM VENDOR t
      WHERE 1 = 1 ${predicate.Builder().CombineOr(
        $predicate.Ctx(0, "AND"),
        $predicate.Ctx(1, "OR" )
      ).Build("AND")}
    ) vendor