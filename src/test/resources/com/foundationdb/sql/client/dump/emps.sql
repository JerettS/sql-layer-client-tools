--- dept

-- IGNORE ERRORS
ALTER TABLE emp DROP FOREIGN KEY `__fk_1`;
DROP TABLE IF EXISTS dept;

CREATE TABLE dept(
  deptno INT NOT NULL,
  CONSTRAINT dept_pkey PRIMARY KEY (deptno),
  name VARCHAR(16) CHARACTER SET utf8 COLLATE ucs_binary NOT NULL,
  dept_head INT
);



INSERT INTO dept VALUES(1, 'G & A', 1),
                       (2, 'Engineering', 3),
                       (3, 'Marketing', 5);

--- emp

DROP TABLE IF EXISTS memo;
DROP TABLE IF EXISTS emp;

CREATE TABLE emp(
  empno INT NOT NULL,
  CONSTRAINT emp_pkey PRIMARY KEY (empno),
  name VARCHAR(128) CHARACTER SET utf8 COLLATE ucs_binary NOT NULL,
  mgr INT,
  deptno INT
);



INSERT INTO emp VALUES(1, 'Smith', NULL, 1),
                      (2, 'Smythe', 1, 1),
                      (3, 'Jones', 1, 2),
                      (4, 'Johnson', 3, 2),
                      (5, 'Adams', 1, 3),
                      (6, 'Addams', 5, 3);

--- memo


CREATE TABLE memo(
  id INT NOT NULL,
  CONSTRAINT memo_pkey PRIMARY KEY (id),
  body TEXT CHARACTER SET utf8 COLLATE ucs_binary,
  author INT
);



INSERT INTO memo VALUES(1, 'blah blah', 3);

ALTER TABLE dept ADD CONSTRAINT dh FOREIGN KEY(dept_head) REFERENCES emp(empno);
ALTER TABLE emp ADD CONSTRAINT `__fk_1` FOREIGN KEY(deptno) REFERENCES dept(deptno);
ALTER TABLE emp ADD CONSTRAINT `__fk_2` FOREIGN KEY(mgr) REFERENCES emp(empno);
ALTER TABLE memo ADD CONSTRAINT `__fk_3` FOREIGN KEY(author) REFERENCES emp(empno) ON DELETE CASCADE;
