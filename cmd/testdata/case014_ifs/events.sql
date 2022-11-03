SELECT events.*
FROM (
         SELECT id
         FROM events
         WHERE 1 = 1
        #if($Unsafe.all)
        AND 1 = 1
        #elseif($Unsafe.none)
        AND 1 = 0
        #end
     ) events