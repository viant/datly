SET GLOBAL log_bin_trust_function_creators = 1;
SET GLOBAL sql_mode = '';

DROP TABLE IF EXISTS USER;
CREATE TABLE USER (
    ID         INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    NAME       VARCHAR(255),
    MGR_ID     INT,
    ACCOUNT_ID INT
);

DROP TABLE IF EXISTS VENDOR;
CREATE TABLE VENDOR (
    ID           INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    NAME         VARCHAR(255),
    ACCOUNT_ID   INT,
    CREATED      DATETIME,
    USER_CREATED INT,
    UPDATED      DATETIME,
    USER_UPDATED INT
);

DROP TABLE IF EXISTS PRODUCT;

CREATE TABLE PRODUCT (
    ID           INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    NAME         VARCHAR(255),
    VENDOR_ID    INT,
    STATUS       INT,
    CREATED      DATETIME,
    USER_CREATED INT,
    UPDATED      DATETIME,
    USER_UPDATED INT
);

DROP TABLE IF EXISTS PRODUCT_JN;

CREATE TABLE PRODUCT_JN (
    PRODUCT_ID INT NOT NULL,
    USER_ID    INT,
    OLD_VALUE  VARCHAR(255),
    NEW_VALUE  VARCHAR(255),
    CREATED    DATETIME
);

DROP FUNCTION IF EXISTS IS_VENDOR_AUTHORIZED;

DELIMITER $$
CREATE FUNCTION IS_VENDOR_AUTHORIZED(USER_ID INT, VENDOR_ID INT)
    RETURNS BOOLEAN
BEGIN
    DECLARE
IS_AUTH BOOLEAN;
SELECT TRUE
INTO IS_AUTH
FROM VENDOR v
WHERE ID = VENDOR_ID
  AND ACCOUNT_ID
  AND EXISTS(SELECT 1 FROM USER u WHERE u.ID = USER_ID AND u.ACCOUNT_ID = v.ACCOUNT_ID);
RETURN IS_AUTH;
END $$
DELIMITER;


DROP FUNCTION IF EXISTS IS_PRODUCT_AUTHORIZED;

DELIMITER $$
CREATE FUNCTION IS_PRODUCT_AUTHORIZED(USER_ID INT, PID INT)
    RETURNS BOOLEAN
BEGIN
    DECLARE
IS_AUTH BOOLEAN;
    SET
IS_AUTH = FALSE ;
SELECT TRUE
INTO IS_AUTH
FROM VENDOR v
         JOIN PRODUCT p ON v.ID = p.VENDOR_ID
WHERE p.ID = PID
  AND ACCOUNT_ID
  AND EXISTS(SELECT 1
             FROM USER u
             WHERE u.ID = USER_ID
               AND u.ACCOUNT_ID = v.ACCOUNT_ID);
RETURN IS_AUTH;
END $$
DELIMITER;


DROP TABLE IF EXISTS DISTRICT;
CREATE TABLE DISTRICT (
    ID   INT PRIMARY KEY,
    NAME VARCHAR(255)
);

DROP TABLE IF EXISTS CITY;
CREATE TABLE CITY (
    ID          INT PRIMARY KEY,
    NAME        varchar(255),
    ZIP_CODE    varchar(255),
    DISTRICT_ID INT
);

DROP TABLE IF EXISTS TEAM;
CREATE TABLE TEAM (
    ID   INT PRIMARY KEY,
    NAME varchar(255),
    ACTIVE INTEGER
);

DROP TABLE IF EXISTS USER_TEAM;
CREATE TABLE USER_TEAM (
    ID      INT PRIMARY KEY,
    USER_ID INT,
    TEAM_ID INT
);

DROP TABLE IF EXISTS EVENTS;
CREATE TABLE EVENTS (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    NAME varchar(255),
    QUANTITY INT
);

DROP TABLE IF EXISTS EVENTS_PERFORMANCE;
CREATE TABLE EVENTS_PERFORMANCE
(
    ID        INT AUTO_INCREMENT PRIMARY KEY,
    PRICE     INT,
    EVENT_ID  INT,
    TIMESTAMP DATE,
    FOREIGN KEY (EVENT_ID) REFERENCES EVENTS (ID)
);

DROP TABLE IF EXISTS FOOS;
CREATE TABLE FOOS (
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        NAME varchar(255),
                        QUANTITY INT
);

DROP TABLE IF EXISTS FOOS_CHANGES;
CREATE TABLE FOOS_CHANGES (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    PREVIOUS TEXT
);

DROP TABLE IF EXISTS FOOS_PERFORMANCE;
CREATE TABLE FOOS_PERFORMANCE (
                      ID INT AUTO_INCREMENT PRIMARY KEY,
                      PERF_NAME varchar(255),
                      PERF_QUANTITY INT,
                      FOO_ID INT,
                      FOREIGN KEY (FOO_ID) REFERENCES FOOS(ID)
);

DROP TABLE IF EXISTS DIFF_JN;
CREATE TABLE DIFF_JN (
  ID INT AUTO_INCREMENT PRIMARY KEY,
  DIFF LONGTEXT
);

DROP TABLE IF EXISTS USER_METADATA;
CREATE TABLE USER_METADATA (
  ID INT AUTO_INCREMENT PRIMARY KEY,
  USER_ID INT,
  IS_ENABLED BIT,
  IS_ACTIVATED BIT,
  FOREIGN KEY (USER_ID) REFERENCES USER (ID)
);

DROP TABLE IF EXISTS OBJECTS;
CREATE TABLE OBJECTS (
                              ID INT AUTO_INCREMENT PRIMARY KEY,
                              OBJECT TEXT,
                              CLASS_NAME VARCHAR(255)
);

DROP TABLE IF EXISTS BAR;
CREATE TABLE BAR (
                      ID INT AUTO_INCREMENT PRIMARY KEY,
                      NAME varchar(255),
                      PRICE DOUBLE PRECISION,
                      TAX FLOAT
);

DROP TABLE IF EXISTS DATLY_JOBS;

CREATE TABLE `DATLY_JOBS` (
                              `MatchKey` varchar(32000) NOT NULL,
                              `Status` varchar(40) NOT NULL,
                              `Metrics` text NOT NULL,
                              `Connector` varchar(256),
                              `TableName` varchar(256),
                              `TableDataset` varchar(256),
                              `TableSchema` varchar(256),
                              `CreateDisposition` varchar(256),
                              `Template` varchar(256),
                              `WriteDisposition` varchar(256),
                              `Cache` text,
                              `CacheKey` varchar(256),
                              `CacheSet` varchar(256),
                              `CacheNamespace` varchar(256),
                              `Method` varchar(256) NOT NULL,
                              `URI`  varchar(256) NOT NULL,
                              `State` text NOT NULL,
                              `UserEmail` varchar(256),
                              `UserID` varchar(256),
                              `MainView` varchar(256) NOT NULL,
                              `Module` varchar(256) NOT NULL,
                              `Labels` varchar(256) NOT NULL,
                              `JobType` varchar(256) NOT NULL,
                              `EventURL` varchar(256) NOT NULL,
                              `Error` text,
                              `CreationTime` datetime NOT NULL,
                              `StartTime` datetime DEFAULT NULL,
                              `ExpiryTime` datetime DEFAULT NULL,
                              `EndTime` datetime DEFAULT NULL,
                              `WaitTimeInMcs` int(11) NOT NULL,
                              `RuntimeInMcs` int(11) NOT NULL,
                              `SQLQuery` text NOT NULL,
                              `Deactivated` tinyint(1),
                              `ID` varchar(40) NOT NULL,
                              PRIMARY KEY (`ID`)
);

CREATE INDEX DATLY_JOBS_REF ON DATLY_JOBS(MatchKey, CreationTime, Deactivated);
