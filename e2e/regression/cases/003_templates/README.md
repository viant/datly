## Data view templates

The following use case [test](test.yaml) data view templates with 3 separate rules matching shared Path prefix.

- [Path: /case_{$parentIndex}/events](rule/events.yaml)
- [Path: /case_{$parentIndex}/events/{id}](rule/event.yaml)
- [Path: /case_${parentIndex}/events/{id}/type](rule/type.yaml)

Each of the rule defines data view output and specific attributes. 

The shared template is defined as follow:

[tmpl/events.yaml](tmpl/events.yaml)
```yaml
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
    connector: db1
```