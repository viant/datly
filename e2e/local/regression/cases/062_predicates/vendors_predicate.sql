/* { "URI":"vendors-predicate", "Method": "GET" } */

#set($_ = $ID<int>(query/ID).WithPredicate(0, "equal", "t", "ID").Optional())
#set($_ = $UserCreated<int>(query/UserCreated).WithPredicate(0, "equal", "t", "USER_CREATED").Optional())
#set($_ = $Name<string>(query/Name).WithPredicate(1, "equal", "t", "NAME").Optional())
#set($_ = $AccountID<int>(query/AccountID).WithPredicate(1, "equal", "t", "ACCOUNT_ID").Optional())
#set( $_ = $Data<?>(output/view).Embed())


SELECT vendor.*
FROM (SELECT *
      FROM VENDOR t
      WHERE 1 = 1 ${predicate.Builder().CombineOr(
        $predicate.FilterGroup(0, "AND"),
        $predicate.FilterGroup(1, "OR" )
      ).Build("AND")}
    ) vendor