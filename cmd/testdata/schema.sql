DROP TABLE IF EXISTS foos;
CREATE TABLE foos
(
    id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name  text,
    price numeric
);

DROP TABLE IF EXISTS events;
CREATE TABLE events
(
    id            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp     DATETIME,
    event_type_id INTEGER,
    quantity      DECIMAL(7, 2) DEFAULT NULL,
    user_id       INTEGER,
    FOREIGN KEY (event_type_id) REFERENCES event_types (id)
);


DROP TABLE IF EXISTS event_types;
CREATE TABLE event_types
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name       VARCHAR(255),
    account_id INTEGER
);