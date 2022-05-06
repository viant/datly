DROP TABLE IF EXISTS departments;
CREATE TABLE departments
(
    id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    name text
);

DROP TABLE IF EXISTS employees;
CREATE TABLE employees
(
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    dep_id INTEGER NOT NULL,
    email  TEXT,
    FOREIGN KEY (dep_id) REFERENCES departments (id)
);

DROP TABLE IF EXISTS roles;
CREATE TABLE roles
(
    id   INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    role text
);

DROP TABLE IF EXISTS emp_roles;
CREATE TABLE emp_roles
(
    id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    role_id     INTEGER NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees (id),
    FOREIGN KEY (role_id) REFERENCES roles (id)
);

DROP TABLE IF EXISTS acl_role_criteria;
CREATE TABLE acl_role_criteria
(
    id       INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    role_id  INTEGER NOT NULL,
    criteria text,
    view     text,
    FOREIGN KEY (role_id) REFERENCES roles (id)
);

DROP TABLE IF EXISTS acl_user_criteria;
CREATE TABLE acl_user_criteria
(
    id          INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    employee_id INTEGER NOT NULL,
    view        text,
    criteria    text,
    FOREIGN KEY (employee_id) REFERENCES employees (id)
);

DROP TABLE IF EXISTS emp_dep;
CREATE TABLE emp_dep
(
    id            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    employee_id   INTEGER NOT NULL,
    department_id INTEGER NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees (id),
    FOREIGN KEY (department_id) REFERENCES departments (id)
);

DROP TABLE IF EXISTS events;
CREATE TABLE events
(
    id            INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    timestamp     DATETIME,
    event_type_id INTEGER,
    quantity      DECIMAL(7, 2) DEFAULT NULL,
    user_id       INTEGER
);
