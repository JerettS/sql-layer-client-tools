\x
SELECT 1,2,3,4;
\x
SELECT 1,2,3,4;
\x
SELECT 1 foo, 2 a;

CREATE TABLE t ( label int, long_label int, a int);
INSERT INTO t (label, long_label, a) values (1,2,3), (4,5,6), (7,8,9);
SELECT * FROM t;

\x on
\x ON
\x off
\x OFF
\x hi
