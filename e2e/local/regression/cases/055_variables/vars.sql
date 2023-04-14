/* {"URI":"vars/",   "Const": { "Var1": "setting1", "Var2": "setting2" }} */
SELECT main.*
FROM (
    SELECT Key1, Key2 FROM (SELECT '$Var1' AS Key1, '$Var2' AS Key2) t
) main
