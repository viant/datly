/* {"URI":"teams",
    "Method": "PUT"
 } */


#set($teamStatsIndex = $Unsafe.TeamStats.IndexBy("ID") /*
    {"Required": false}
    SELECT
        t.ID,
        (
            CASE
            WHEN ut.TEAM_ID IS NULL THEN 0
            ELSE COUNT(1)
            END
        )  as TEAM_MEMBERS,
        t.NAME as NAME
    FROM TEAM t
    LEFT JOIN USER_TEAM ut ON t.ID = ut.TEAM_ID
    WHERE t.ID IN ($TeamIDs)
    GROUP BY t.ID
*/)

#foreach($teamID in $Unsafe.TeamIDs)
   #if($teamStatsIndex.HasKey($teamID) == false)
    $logger.Fatal("not found team with ID %v", $teamID)
   #end

   #set($aTeam = $teamStatsIndex[$teamID])
   #if($aTeam.TEAM_MEMBERS != 0)
    $logger.Fatal("can't deactivate team %v with %v members", $aTeam.NAME, $aTeam.TEAM_MEMBERS)
   #end
UPDATE TEAM
SET ACTIVE = false
WHERE ID = $teamID;
#end
