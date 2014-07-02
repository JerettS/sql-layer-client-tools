DROP SEQUENCE IF EXISTS new_sequence RESTRICT;
DROP SEQUENCE IF EXISTS new_sequence2 RESTRICT;

CREATE SEQUENCE new_sequence START WITH 1 INCREMENT BY 1 MINVALUE 0 MAXVALUE 100000 CYCLE;
CREATE SEQUENCE new_sequence2 START WITH 1 INCREMENT BY 1 MINVALUE 0 MAXVALUE 100000 CYCLE;


--- seq_test

DROP TABLE IF EXISTS seq_test;

CREATE TABLE seq_test(
  cid INT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1),
  CONSTRAINT seq_test_pkey PRIMARY KEY (cid),
  name VARCHAR(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL
);




--- t4

DROP TABLE IF EXISTS t4;

CREATE TABLE t4(
  id INT NOT NULL,
  CONSTRAINT t4_pkey PRIMARY KEY (id),
  s VARCHAR(10) CHARACTER SET utf8 COLLATE ucs_binary
);



INSERT INTO t4 VALUES(3, 'barf'),
                     (4, 'two');

ALTER TABLE t4 ALTER COLUMN id SET GENERATED ALWAYS AS IDENTITY (START WITH 6, INCREMENT BY 1);

