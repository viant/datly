



$sequencer.Allocate("EVENTS", $Unsafe.Events, "Id")
INSERT INTO EVENTS( 
ID, 
NAME, 
QUANTITY
) VALUES (
 $criteria.AppendBinding($Unsafe.Events.Id), 
 $criteria.AppendBinding($Unsafe.Events.Name), 
 $criteria.AppendBinding($Unsafe.Events.Quantity)
);
