CREATE TABLE IF NOT EXISTS `%v` (
                                `JOB_ID` VARCHAR(40) NOT NULL,
                                `QUALIFIER` TEXT,
                                `STATE` TEXT NOT NULL,
                                `VALUE` TEXT,
                                `ERROR` TEXT,
                                `CREATION_TIME` DATE NOT NULL,
                                `END_TIME` DATE NOT NULL,
                                PRIMARY KEY (`JOB_ID`)
)