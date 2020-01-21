## Data view references

This use case [test](test.yaml) various aspects data view references:

##### Reference with 'One' cardinality

The [events.yaml](rule/events.yaml) rule defines an event complex object with:
- cardinality One
- hideRefIds flag only on event_type data view thus events object will return both event_type_id field and type object,
but event_type object will only return account object but not account_id field.  


[events.yaml](rule/events.yaml):

```yaml
uri: /case_${parentIndex}/events
views:
  - table: events
    alias: e
    connector: db1
    refs:
      - name: type
        cardinality: One
        dataView: event_types
            'on':
              - column: event_type_id
                refColumn: id

  - table: event_types
    hideRefIds: true
    alias: t
    connector: db1
    refs:
      - name: account
        cardinality: One
        dataView: accounts
        'on':
          - column: account_id
            refColumn: id

  - table: accounts
    alias: a
    selector:
      prefix: acc
    connector: db1
```

######## Data View selectors

Apply selector test controls data view behaviour with with the following query string parameters:

- client side query string based data view selector: 
    * _fields=id,quantity,event_type_id (applied to main events data view)   
    * _orderBy=id   (applied to main events data view)
    * _criteria=id>3 (applied to main events data view)
    * event_types_fields=id,name (applied to event_types data view)
    * acc_fields=id,name (applied to accounts data view via defined selector prefix)
  

##### Reference with 'Many' cardinality

The [event_types.yaml](rule/event_types.yaml) rule defines event_type complex object with:

- cardinality Many
- difference connector (db1: mysql, db2: postgres)

[event_types.yaml](rule/event_types.yaml):
```yaml
uri: /case_${parentIndex}/event_types
views:
  - table: event_types
    alias: t
    connector: db1
    refs:
      - name: evnts
        cardinality: Many
        dataView: events
        'on':
          - column: id
            refColumn: event_type_id

  - table: events
    alias: e
    connector: db2
```
