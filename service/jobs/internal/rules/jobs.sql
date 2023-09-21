/* {
   "URI":"/{Source}/",
   "Include":[
        "shared/predicates.yaml",
        "shared/input.sqlx",
        "shared/output.sqlx"
   ]
} */

SELECT jobs.*
FROM (SELECT *
      FROM DATLY_JOBS t
      WHERE 1 = 1
          ${predicate.Builder().CombineOr(
             $predicate.FilterGroup(0, "AND")
         ).Build("AND")}
) jobs

