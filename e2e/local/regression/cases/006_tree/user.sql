/* {"URI":"users/"} */
SELECT user.* EXCEPT MGR_ID /* {"Style":"Comprehensive", "Field":"Data"}  */
FROM (SELECT t.* FROM USER t  ) user /* {"Self":{"Holder":"Team", "Child":"ID", "Parent":"MGR_ID" }} */
