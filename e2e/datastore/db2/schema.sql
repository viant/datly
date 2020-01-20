DROP TABLE IF EXISTS events;
CREATE TABLE events
(
  id           INT
  CONSTRAINT events_id PRIMARY KEY,
  event_type_id   INT,
  quantity     DECIMAL(12, 7) DEFAULT NULL,
  timestamp    TIMESTAMP,
  query_string TEXT
);



DROP TABLE IF EXISTS event_types;
CREATE TABLE event_types
(
  id           INT
  CONSTRAINT event_types_id PRIMARY KEY,
  account_id INT,
  name VARCHAR(255)
);



DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
      id           INT
    CONSTRAINT accounts_id PRIMARY KEY,
    name       VARCHAR(255)
);