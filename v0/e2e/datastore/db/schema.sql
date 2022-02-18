    DROP TABLE IF EXISTS events;
CREATE TABLE events
(
  id           INT AUTO_INCREMENT PRIMARY KEY,
  event_type_id   INT,
  quantity     DECIMAL(10,7),
  timestamp    TIMESTAMP,
  query_string VARCHAR(255)
);


DROP TABLE IF EXISTS event_types;
CREATE TABLE event_types
(
  id           INT AUTO_INCREMENT PRIMARY KEY,
  account_id INT,
  name VARCHAR(255),
  modified    TIMESTAMP
);



DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    name       VARCHAR(255),
    modified    TIMESTAMP
);