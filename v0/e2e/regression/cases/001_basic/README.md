## Basic data view functionality

This use case [test](test.yaml) basis data view functionality.


##### Path matching test

In use case the following rule Paths are defined each matching individual rule:
* [/case_${parentIndex}/events](rule/events.yaml)   
* [/case_${parentIndex}/event_type/{id}](rule/event_types.yaml)
* [/case_${parentIndex}/events_and_types](rule/events_and_types.yaml)

If rule defines more than one data view, data reading is perform concurrently.

