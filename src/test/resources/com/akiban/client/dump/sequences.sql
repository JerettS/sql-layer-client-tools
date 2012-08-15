DROP SEQUENCE IF EXISTS dump_test.new_sequence RESTRICT;

CREATE SEQUENCE dump_test.new_sequence START WITH 1 INCREMENT BY 1 MINVALUE 0 MAXVALUE 100000 CYCLE;
--- seq_test

DROP TABLE IF EXISTS seq_test;

CREATE TABLE seq_test(
  cid INT NOT NULL GENERATED BY DEFAULT AS IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY,
  name VARCHAR(32) CHARACTER SET latin1 COLLATE latin1_swedish_ci NOT NULL
);




