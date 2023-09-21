/* {
   "URI":"/jobs/{Source}"
   "Include":[
        "jobs/predicates.yaml",
        "jobs/input.sqlx",
        "jobs/output.sqlx"
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

