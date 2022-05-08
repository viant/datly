CREATE TABLE datly.acl_role
(
    id      INT64,
    name    STRING,
    created TIMESTAMP,
    updated TIMESTAMP
);


CREATE TABLE datly.acl_subject_role
(
    role_id  INT64,
    subject  STRING,
    comments STRING,
    created  TIMESTAMP,
    updated  TIMESTAMP
);


CREATE TABLE datly.acl_role_criteria
(
    role_id  INT64,
    criteria STRING,
    view     STRING,
    comments STRING,
    created  TIMESTAMP,
    updated  TIMESTAMP
);


CREATE TABLE datly.acl_subject_criteria
(
    subject  STRING,
    view     STRING,
    criteria STRING,
    comments STRING,
    created  TIMESTAMP,
    updated  TIMESTAMP
);


CREATE OR REPLACE VIEW datly.acl_view_criteria AS
SELECT ac.criteria, ac.view, ac.subject, 'd' AS source
FROM datly.acl_subject_criteria ac
UNION ALL
SELECT rc.criteria,  rc.view, sr.subject, 'r' AS source
FROM datly.acl_role_criteria rc
JOIN datly.acl_subject_role sr ON rc.role_id = sr.role_id
ORDER BY source