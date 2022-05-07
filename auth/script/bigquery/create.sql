DROP TABLE IF EXISTS acl_role;
CREATE TABLE acl_role (
    id      INT64,
    name    STRING
        created TIMESTAMP,
    updated TIMESTAMP
);


DROP TABLE IF EXISTS acl_subject_role;
CREATE TABLE acl_subject_role (
    role_id  INT64,
    subject  STRING,
    comments STRING
        created TIMESTAMP,
    updated  TIMESTAMP
);


DROP TABLE IF EXISTS acl_role_criteria;
CREATE TABLE acl_role_criteria (
    role_id  INT64,
    criteria STRING,
    view     STRING
);



DROP TABLE IF EXISTS acl_subject_criteria;
CREATE TABLE acl_subject_criteria (
    subject  STRING,
    view     STRING,
    criteria text
);


