CREATE TABLE t ( label int, long_label int, a int);
INSERT INTO t (label, long_label, a) values (1,NULL,3), (4,NULL,6), (7,8,NULL);
SELECT * FROM t;

\a OFF

SELECT * FROM t;

\x

SELECT * FROM t;
\a
SELECT * FROM t;

\x

\a
\a STRING


