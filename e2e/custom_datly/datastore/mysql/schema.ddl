DROP TABLE IF EXISTS FOOS;
CREATE TABLE FOOS (
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        NAME varchar(255),
                        QUANTITY INT
);

DROP TABLE IF EXISTS FOOS_PERFORMANCE;
CREATE TABLE FOOS_PERFORMANCE (
                                  ID INT AUTO_INCREMENT PRIMARY KEY,
                                  PERF_NAME varchar(255),
                                  PERF_QUANTITY INT,
                                  FOO_ID INT,
                                  FOREIGN KEY (FOO_ID) REFERENCES FOOS(ID)
);