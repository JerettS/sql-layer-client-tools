CREATE TABLE t1(c1 int default 10, c2 varchar(32) character set utf8 collate en_ci);
CREATE TABLE t2(c1 decimal(10,5), c2 varchar(10) for bit data);
CREATE INDEX i1 ON t1(c1);
CREATE INDEX i2 ON t2(c1);
CREATE SEQUENCE s1;
CREATE SEQUENCE s2;
CREATE VIEW v1 AS SELECT * FROM t1;
CREATE VIEW v2 AS SELECT * FROM t2;
  \l test.foo
\l test.%
\l test.%1
\l test.t1
\l test.s1
\l test.v1
\li test.%.%
\li test.t1.%
\li test.t1.i1
\lq test.%
\lq test.s1
\lt test.%
\lt test.t1
\lv test.%
\lv test.v1
\dt test.%
\dt test.t1
\dt+ test.t1
\dv test.%
\dv test.v1
\dv+ test.v1
\xfoox

\timing
SELECT * from t1;
\timing
SELECT * from t1;

\q
SELECT 'should not get here';
