DROP TABLE IF EXISTS events;
CREATE TABLE events
(
    id            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp     DATETIME,
    event_type_id INTEGER,
    quantity      DECIMAL(7, 2) DEFAULT NULL,
    user_id       INTEGER
);


DROP TABLE IF EXISTS event_types;
CREATE TABLE event_types
(
    id         INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name       VARCHAR(255),
    account_id INTEGER
);


DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts
(
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name    VARCHAR(255),
    user_id INTEGER NOT NULL
);

DROP TABLE IF EXISTS users;
CREATE TABLE users
(
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name    VARCHAR(255)
);

DROP TABLE IF EXISTS foos;
CREATE TABLE foos
(
    id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name  VARCHAR(255),
    price DECIMAL(7, 2)
);