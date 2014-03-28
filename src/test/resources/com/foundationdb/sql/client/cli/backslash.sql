CREATE TABLE t1(c1 int default 10, c2 varchar(32) character set utf8 collate en_ci);
CREATE TABLE t2(c1 decimal(10,5), c2 varchar(10) for bit data);
CREATE INDEX i1 ON t1(c1);
CREATE INDEX i2 ON t2(c1);
CREATE SEQUENCE s1;
CREATE SEQUENCE s2;
CREATE VIEW v1 AS SELECT * FROM t1;
CREATE VIEW v2 AS SELECT * FROM t2;

CREATE TABLE c(cid INT NOT NULL PRIMARY KEY);
CREATE TABLE a(cid INT NOT NULL, oid INT NOT NULL, PRIMARY KEY(cid, oid), UNIQUE(oid), GROUPING FOREIGN KEY(cid) REFERENCES c);
CREATE TABLE o(oid INT NOT NULl PRIMARY KEY, cid INT, GROUPING FOREIGN KEY(cid) REFERENCES c);
CREATE TABLE i(iid INT NOT NULL PRIMARY KEY, oid INT, GROUPING FOREIGN KEY(oid) REFERENCES o);
ALTER TABLE o ADD CONSTRAINT cfk FOREIGN KEY(cid) REFERENCES c;
ALTER TABLE i ADD CONSTRAINT ofk FOREIGN KEY(oid) REFERENCES o;


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

\d test.%
\dq test.%

\dt test.%
\dt test.t1
\dt+ test.t1

\dv test.%
\dv test.v1
\dv+ test.v1

\xfoox

\lt test.%;
\lt test.% ;

\q
SELECT 'should not get here';
